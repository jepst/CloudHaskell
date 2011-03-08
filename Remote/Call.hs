{-# LANGUAGE TemplateHaskell #-}

module Remote.Call (
-- * Compile-time metadata
         remoteCall,
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
         Closure(..),
         liftIO,serialEncodePure,serialDecode
        ) where

import Language.Haskell.TH
import Data.Maybe (maybe)
import Data.List (intercalate,null)
import qualified Data.Map as Map (Map,insert,lookup,empty,toList) 
import Data.Dynamic (toDyn,Dynamic,fromDynamic,dynTypeRep)
import Data.Generics (Data)
import Data.Typeable (typeOf,Typeable)
import Data.IORef (newIORef,atomicModifyIORef,IORef,readIORef,writeIORef)
import Control.Concurrent.MVar (newMVar,takeMVar,putMVar,readMVar,MVar)
import Data.Binary (Binary)
import Remote.Encoding (Payload,serialDecode,serialEncodePure,Serializable)
import Control.Monad.Trans (liftIO)
import Remote.Closure (Closure(..))

import Debug.Trace

----------------------------------------------
-- * Compile-time metadata
----------------------------------------------

type RemoteCallMetaData = Lookup -> Lookup

-- Language.Haskell.TH.recover seems to not work at all for syntax errors
-- during splicing as I would expect. Not really sure what it's good for, 
-- then. It would be nice if we could have some kind of try/catch in here.

-- | This is a compile-time function that automatically generates
--   metadata for declarations passed to it as an argument. It's designed
--   to be used within your source code like this:
--
-- > remoteCall 
-- >    [d|
-- >       myFun :: Int -> ProcessM ()
-- >       myFun = ...
-- >    |]
--
-- If you are using an older version of GHC, you may have to use this
-- syntax instead:
--
-- > $( remoteCall 
-- >    [d|
-- >       myFun :: Int -> ProcessM ()
-- >       myFun = ...
-- >    |] )
--
-- Both of these forms require enabling Template Haskell with the
-- @-XTemplateHaskell@ compiler flag or @{-\# LANGUAGE TemplateHaskell \#-}@
-- pragma.
--
-- This function generates the following additional identifiers:
--
-- 1. Each 'remoteCall' block emits a table of function names
--    used to invoke closures. The table is generated under the
--    name '__remoteCallMetaData'. All such tables, one per module,
--    must be registered when creating a node with 'Remote.Process.initNode'
--    by calling 'registerCalls'. For example:
--
--    > let lookup = registerCalls [Main.__remoteCallMetaData, Foobar.__remoteCallMetaData]
--
--    It is the programmer's responsibility to ensure that these names
--    are visible at the location where they will be used.
--
-- 2. If a function has an explicit type signature and is in the
--    'ProcessM' monad, a function is created to generate a closure
--    for that function, suitable for use with 'Remote.Process.spawnRemote'.
--    This function has the name of the original function combined with
--    the suffix @__closure@, and a return value equal to the original enclosed
--    in a 'Remote.Closure.Closure'. For example, given the above definition
--    of 'myFun', the closure-generation function would have the
--    the following signature:
--
--    > myFun__impl :: Int -> Closure (ProcessM ())
--
-- You can use 'remoteCall' only once per module. All declarations
-- for which metadata is to be generated must be contained within a
-- single block.
remoteCall :: Q [Dec] -> Q [Dec]
remoteCall a = recover (errorMessage) (buildMeta $ buildMeta2 a)

buildMeta2 :: Q [Dec] -> Q [Dec]
buildMeta2 qdecs = do decs <- qdecs
                      loc <- location
                      res <- mapM (fixDec loc) decs
                      return $ concat res
   where fixDec loc dec = 
                     case dec of
                         SigD name typ -> let implName = mkName (nameBase name ++ "__impl") 
                                              implFqn = loc_module loc ++"." ++ nameBase name ++ "__impl"
                                              closureName = mkName (nameBase name ++ "__closure")
                                              closurearglist = init arglist ++ [processmtoclosure (last arglist)]
--                                              processmtoclosure (AppT (ConT _) x) = (AppT (ConT (mkName "Remote.Process.ProcessM")) (AppT (ConT (mkName "Remote.Closure.Closure")) x))
                                              processmtoclosure (x) =  (AppT (ConT (mkName "Remote.Call.Closure")) x)
--                                              processmtoclosure _ = error "Unexpected type in closure"
                                              paramnames = map (\x -> 'a' : show x) [1..(length (init arglist))]
                                              paramnamesP = (map (varP . mkName) paramnames)
                                              paramnamesE = (map (varE . mkName) paramnames)
                                              payload = [t| Remote.Encoding.Payload |]
                                              just a = conP (mkName "Just") [a]
                                              liftio = [e| Control.Monad.Trans.liftIO |]
                                              decodecall = [e| Remote.Encoding.serialDecode |]
                                              encodecall = [e| Remote.Encoding.serialEncodePure |]
                                              closurecall = conE (mkName "Remote.Call.Closure")
                                              returncall = [e| return |]
                                              arglist = getParams typ
                                              errorcall = [e| error |]
                                              applyargs f [] = f
                                              applyargs f (l:r) = applyargs (appE f l) r
                                              closuredec = sigD closureName (return $ putParams closurearglist)
                                              closuredef = funD closureName [clause paramnamesP
                                                                             (normalB (appE (appE closurecall (litE (stringL implFqn))) (appE encodecall (tupE paramnamesE))))
                                                                             []
                                                                            ]
                                              impldec = sigD implName (appT (appT arrowT payload) (return $ last arglist))
                                              impldef = funD implName [clause [varP (mkName "a")]
                                                                              (normalB (doE [bindS (varP (mkName "res")) (appE liftio (appE decodecall (varE (mkName "a")))),
                                                                                            noBindS (caseE (varE (mkName "res"))
                                                                                                  [match (just (tupP paramnamesP)) (normalB (applyargs (varE name) paramnamesE)) [],
                                                                                                   match wildP (normalB (appE (errorcall) (litE (stringL ("Bad decoding in closure splice of "++nameBase name))))) []])
                                                                                           ]))
                                                                              []]
                                             in case (last arglist) of
-- TODO this line currently prevents (in a very hacky way) generation of closure code 
-- for non-ProcessM functions. I can imagine that you might want closures for
-- pure code, which might be invoked remotely with an auto-restart clause, though.
                                                   (AppT (ConT process) _) | show process == "Remote.Process.ProcessM" ->
                                                         do v <- sequence [closuredec,closuredef,impldec,impldef]
                                                            return $ dec:v
                                                   _ -> return [dec]
                         _ -> return [dec]
         putParams (afst:lst:[]) = AppT (AppT ArrowT afst) lst
         putParams (afst:[]) = afst
         putParams (afst:lst) = AppT (AppT ArrowT afst) (putParams lst)
         getParams typ = case typ of
                                  AppT (AppT ArrowT b) c -> b : getParams c
                                  b -> [b]

errorLocation :: Loc -> String
errorLocation loc = let file = loc_filename loc
                        (x1,y1) = loc_start loc
                        (x2,y2) = loc_end loc in
                    (intercalate ":" [file,show x1,show y1]) ++ "-" ++ (intercalate ":" [show x2,show y2])

errorMessage :: Q a
errorMessage = do
         loc <- location
         error ("Remote.Call.remoteCall failed at compile-time at "++errorLocation loc++". Is it possible that you've invoked this function more than once per module in module "++loc_module loc++"?")

collectNames :: [Dec] -> [Name]
collectNames decs = foldl each [] decs
  where each entries dec = case dec of
                              (FunD name _) -> name:entries
                              (ValD (VarP name) _ _) -> name:entries
                              _ -> entries

patchDecls :: Q [Dec] -> Q [Dec]
patchDecls decls = decls

buildMeta :: Q [Dec] -> Q [Dec]
buildMeta qdecls = do decls <- patchDecls qdecls
                      loc <- location
                      let names = collectNames decls
                      let bind = [e| (=<<) |]
--                      let register = [e| Remote.Call.register |]
                      let mkentry = [e| Remote.Call.putReg |]
                      let thetype = [t| RemoteCallMetaData |]
                      let iovoid = [t| IO () |]
                      let patq = (mkName "__remoteCallMetaData")
                      let reasonableNameModule name = maybe (loc_module loc++".") ((++)".") (nameModule name)
                      let app2Ei op l r = infixE (Just l) op (Just r)
                      let app2E op l r = appE (appE op l) r
                      param <- newName "x"
                      let applies [] = varE param
                          applies [h] = appE (app2E mkentry (varE h) (litE $ stringL (reasonableNameModule h++nameBase h))) (varE param)
                          applies (h:t) = appE (app2E mkentry (varE h) (litE $ stringL (reasonableNameModule h++nameBase h))) (applies t)
                      let bodyq = normalB (applies names)
                      
                      sig <- sigD patq thetype
                      dec <- funD patq [clause [varP param] bodyq []]
                      if null decls 
                         then return decls
                         else return (sig:dec:decls)

----------------------------------------------
-- * Run-time metadata
----------------------------------------------

type Identifier = String

data Entry = Entry {
               entryName :: Identifier,
               entryFunRef :: Dynamic
             }

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

