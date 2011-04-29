{-# LANGUAGE TemplateHaskell #-}

module Remote.Call (
-- * Compile-time metadata
         remotable,
         mkClosure,
         RemoteCallMetaData,
-- * Runtime metadata
         registerCalls,
         Lookup,
         Identifier,
         putReg,
         getEntryByIdent,
         empty,
-- * Re-exports
         Payload,
         Closure(..)
        ) where

import Language.Haskell.TH
import Data.Maybe (maybe)
import Data.List (intercalate,null)
import qualified Data.Map as Map (Map,insert,lookup,empty,toList) 
import Data.Dynamic (toDyn,Dynamic,fromDynamic,dynTypeRep)
import Data.Generics (Data)
import Data.Typeable (typeOf,Typeable)
import Data.Binary (Binary)
import Remote.Encoding (Payload,serialDecode,serialEncode,serialEncodePure,Serializable)
import Control.Monad.Trans (liftIO)
import Control.Monad (liftM)
import Remote.Closure (Closure(..))

----------------------------------------------
-- * Compile-time metadata
----------------------------------------------

-- | Data of this type is generated at compile-time
-- by 'remotable' and can be used with 'registerCalls'
-- to create a metadata lookup table, 'Lookup'.
-- The name '__remoteCallMetaData' will be present
-- in any module that uses 'remotable'.
type RemoteCallMetaData = Lookup -> Lookup

mkClosure :: Name -> Q Exp
mkClosure n = do info <- reify n
                 case info of
                    VarI iname _ _ _ -> 
                        do let newn = mkName $ show iname ++ "__closure"
                           newinfo <- reify newn
                           case newinfo of
                              VarI newiname _ _ _ -> varE newiname
                              _ -> error $ "Unexpected type of closure symbol for "++show n
                    _ -> error $ "No closure corresponding to "++show n

remotable :: [Name] -> Q [Dec]
remotable names =
    do info <- liftM concat $ mapM getType names 
       loc <- location
       let declGen = map (makeDec loc) info
       decs <- sequence $ concat (map fst declGen)
       let outnames = concat $ map snd declGen
       regs <- sequence $ makeReg loc outnames
       return $ decs ++ regs
    where makeReg loc names = 
              let
                      mkentry = [e| Remote.Call.putReg |]
                      regtype = [t| RemoteCallMetaData |]
                      registryName = (mkName "__remoteCallMetaData")
                      reasonableNameModule name = maybe (loc_module loc++".") ((flip (++))".") (nameModule name)
                      app2E op l r = appE (appE op l) r
                      param = mkName "x"
                      applies [] = varE param
                      applies [h] = appE (app2E mkentry (varE h) (litE $ stringL (reasonableNameModule h++nameBase h))) (varE param)
                      applies (h:t) = appE (app2E mkentry (varE h) (litE $ stringL (reasonableNameModule h++nameBase h))) (applies t)
                      bodyq = normalB (applies names)
                      sig = sigD registryName regtype
                      dec = funD registryName [clause [varP param] bodyq []]
               in [sig,dec]
          makeDec loc (aname,atype) =
              let
                 implName = mkName (nameBase aname ++ "__impl") 
                 implPlName = mkName (nameBase aname ++ "__implPl")
                 implFqn = loc_module loc ++"." ++ nameBase aname ++ "__impl"
                 closureName = mkName (nameBase aname ++ "__closure")
                 paramnames = map (\x -> 'a' : show x) [1..(length (init arglist))]
                 paramnamesP = (map (varP . mkName) paramnames)
                 paramnamesE = (map (varE . mkName) paramnames)
                 closurearglist = init arglist ++ [processmtoclosure (last arglist)]
                 implarglist = payload : [toPayload (last arglist)]
                 toProcessM a = (AppT (ConT (mkName "Remote.Process.ProcessM")) a)
                 toPayload x = case funtype of
                                  0 -> case x of
                                        (AppT (ConT n) _) -> (AppT (ConT n) payload)
                                        _ -> toProcessM payload
                                  _ -> toProcessM payload
                 processmtoclosure (x) =  (AppT (ConT (mkName "Remote.Call.Closure")) x)
                 applyargs f [] = f
                 applyargs f (l:r) = applyargs (appE f l) r
                 funtype = case last arglist of
                              (AppT (ConT process) _) | show process == "Remote.Process.ProcessM" -> 0
                                                      | show process == "GHC.Types.IO" -> 1
                              _ -> 2
                 payload = ConT ( mkName "Remote.Call.Payload")
                 just a = conP (mkName "Prelude.Just") [a]
                 errorcall = [e| Prelude.error |]
                 liftio = [e| Control.Monad.Trans.liftIO |]
                 returnf = [e| Prelude.return |]
                 asProcessM x = case funtype of
                                  0 -> x
                                  1 -> case x of
                                         (AppT (ConT _) a) -> AppT (ConT (mkName "Remote.Process.ProcessM")) a
                                         _ -> AppT (ConT (mkName "Remote.Process.ProcessM")) x
                                  2 -> AppT (ConT (mkName "Remote.Process.ProcessM")) x
                 lifter x = case funtype of
                                0 -> x
                                1 -> appE liftio x
                                _ -> appE returnf x
                 decodecall = [e| Remote.Encoding.serialDecode |]
                 encodecallio = [e| Remote.Encoding.serialEncode |]
                 encodecall = [e| Remote.Encoding.serialEncodePure |]
                 closurecall = [e| Remote.Closure.Closure |]
                 closuredec = sigD closureName (return $ putParams closurearglist)
                 closuredef = funD closureName [clause paramnamesP
                                        (normalB (appE (appE closurecall (litE (stringL implFqn))) (appE encodecall (tupE paramnamesE))))
                                        []
                                       ]
                 impldec = sigD implName (appT (appT arrowT (return payload)) (return $ asProcessM $ last arglist))
                 impldef = funD implName [clause [varP (mkName "a")]
                                         (normalB (doE [bindS (varP (mkName "res")) (appE liftio (appE decodecall (varE (mkName "a")))),
                                                       noBindS (caseE (varE (mkName "res"))
                                                             [match (just (tupP paramnamesP)) (normalB (lifter (applyargs (varE aname) paramnamesE))) [],
                                                              match wildP (normalB (appE (errorcall) (litE (stringL ("Bad decoding in closure splice of "++nameBase aname))))) []])
                                                      ]))
                                         []]

                 implPldec = case last arglist of
                              (AppT (ConT process) v) | show process == "Remote.Task.TaskM" ->
                                   sigD implPlName (return $ putParams $ [payload,(AppT (ConT process) payload)])
                              _ -> sigD implPlName (return $ putParams implarglist)
                 implPldef = case last arglist of
                              (AppT (ConT process) _) | show process == "Remote.Task.TaskM" ->
                                   funD implPlName [clause [varP (mkName "a")]
                                                  (normalB (appE (varE (mkName "Remote.Task.remoteCallRectify")) (appE (varE implName) (varE (mkName "a") )))) []
                                        ]
                              _ -> funD implPlName [clause [varP (mkName "a")]
                                         (normalB (doE [bindS (varP (mkName "res")) ( (appE (varE implName) (varE (mkName "a")))),
                                                       noBindS (appE liftio (appE encodecallio (varE (mkName "res")))) ] )) [] ] 
                 arglist = getParams atype
              in
                 case funtype of
                      _ -> ([closuredec,closuredef,impldec,impldef,implPldec,implPldef],[aname,implName,implPlName])
          getType name = 
             do info <- reify name
                case info of 
                  VarI iname itype _ _ -> return [(iname,itype)]
                  _ -> return []
          putParams (afst:lst:[]) = AppT (AppT ArrowT afst) lst
          putParams (afst:[]) = afst
          putParams (afst:lst) = AppT (AppT ArrowT afst) (putParams lst)
          putParams [] = error "Unexpected parameter type in remotable processing"
          getParams typ = case typ of
                            AppT (AppT ArrowT b) c -> b : getParams c
                            b -> [b]
                                
----------------------------------------------
-- * Run-time metadata
----------------------------------------------

type Identifier = String

data Entry = Entry {
               entryName :: Identifier,
               entryFunRef :: Dynamic
             }

-- | Creates a metadata lookup table based on compile-time metadata.
-- You probably don't want to call this function yourself, but instead
-- use 'Remote.Init.remoteInit'.
registerCalls :: [RemoteCallMetaData] -> Lookup
registerCalls [] = empty
registerCalls (h:rest) = let registered = registerCalls rest
                          in h registered

makeEntry :: (Typeable a) => Identifier -> a -> Entry
makeEntry ident funref = Entry {entryName=ident, entryFunRef=toDyn funref}

type IdentMap = Map.Map Identifier Entry
data Lookup = Lookup { identMap :: IdentMap }

putReg :: (Typeable a) => a -> Identifier -> Lookup -> Lookup
putReg a i l = putEntry l a i

putEntry :: (Typeable a) => Lookup -> a -> Identifier -> Lookup
putEntry amap value name = 
                             Lookup {
                                identMap = Map.insert name entry (identMap amap)
                             }
  where
       entry = makeEntry name value


getEntryByIdent :: (Typeable a) => Lookup -> Identifier -> Maybe a
getEntryByIdent amap ident = (Map.lookup ident (identMap amap)) >>= (\x -> fromDynamic (entryFunRef x))

empty :: Lookup
empty = Lookup {identMap = Map.empty}

