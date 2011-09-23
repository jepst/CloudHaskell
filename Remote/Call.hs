{-# LANGUAGE TemplateHaskell #-}

-- | Provides Template Haskell-based tools
-- and syntactic sugar for dealing with closures
module Remote.Call (
         remotable,
         mkClosure,
         mkClosureRec,
        ) where

import Language.Haskell.TH
import Remote.Encoding (Payload,serialDecode,serialEncode,serialEncodePure)
import Control.Monad.Trans (liftIO)
import Control.Monad (liftM)
import Remote.Closure (Closure(..))
import Remote.Process (ProcessM)
import Remote.Reg (putReg,RemoteCallMetaData)
import Remote.Task (remoteCallRectify,TaskM)

-- TODO this module is the result of months of tiny thoughtless changes and desperately needs a clean-up

----------------------------------------------
-- * Compile-time metadata
----------------------------------------------

-- | A compile-time macro to expand a function name to its corresponding
-- closure name (if such a closure exists), suitable for use with
-- 'spawn', 'callRemote', etc
-- In general, using the syntax @$(mkClosure foo)@ is the same
-- as addressing the closure generator by name, that is,
-- @foo__closure@. In some cases you may need to use
-- 'mkClosureRec' instead.
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


-- | A variant of 'mkClosure' suitable for expanding closures
-- of functions declared in the same module, including that
-- of the function it's used in. The Rec stands for recursive.
-- If you get the @Something is not in scope at a reify@ message
-- when using mkClosure, try using this function instead.
-- Using this function also turns off the static
-- checks used by mkClosure, and therefore you are responsible
-- for making sure that you use 'remotable' with each function
-- that may be an argument of mkClosureRec
mkClosureRec :: Name -> Q Exp
mkClosureRec name =
  do loc <- location
     info <- reify name
     case info of
       VarI aname atype _ _ -> 
        let modname = case nameModule aname of
                        Just a -> a
                        _ -> error "Non-module name at mkClosureRec "++show name
         in
           case modname == loc_module loc of
             True -> let implFqn = loc_module loc ++"." ++ nameBase aname ++ "__impl" 
                         closurecall = [e| Remote.Closure.Closure |]
                         encodecall = [e| Remote.Encoding.serialEncodePure |]
                         paramnames = map (\x -> 'a' : show x) [1..(length (init arglist))]
                         arglist = getParams atype
                         paramnamesP = (map (varP . mkName) paramnames)
                         paramnamesE = (map (varE . mkName) paramnames)
                         paramnamesT = map (\(v,t) -> sigE v (return t)) (zip paramnamesE arglist)
                      in lamE paramnamesP (appE (appE closurecall (litE (stringL implFqn))) (appE encodecall (tupE paramnamesT)))
             False -> error $ show aname++": mkClosureRec can only make closures of functions in the same module; use mkClosure instead"
       _ -> error $ "Unexpected type of symbol for "++show name
     

-- | A compile-time macro to provide easy invocation of closures.
-- To use this, follow the following steps:
--
-- 1. First, enable Template Haskell in the module:
--
--   > {-# LANGUAGE TemplateHaskell #-}
--   > module Main where
--   > import Remote.Call (remotable)
--   >    ...
--
-- 2. Define your functions normally. Restrictions: function's type signature must be explicitly declared; no polymorphism; all parameters must implement Serializable; return value must be pure, or in one of the 'ProcessM', 'TaskM', or 'IO' monads; probably other restrictions as well.
--
--   > greet :: String -> ProcessM ()
--   > greet name = say ("Hello, "++name)
--   > badFib :: Integer -> Integer
--   > badFib 0 = 1
--   > badFib 1 = 1
--   > badFib n = badFib (n-1) + badFib (n-2)
--
-- 3. Use the 'remotable' function to automagically generate stubs and closure generators for your functions:
--
--   > $( remotable ['greet, 'badFib] )
--
--   'remotable' may be used only once per module.
--
-- 4. When you call 'remoteInit' (usually the first thing in your program), 
-- be sure to give it the automagically generated function lookup tables
-- from all modules that use 'remotable':
--
--   > main = remoteInit (Just "config") [Main.__remoteCallMetaData, OtherModule.__remoteCallMetaData] initialProcess
--
-- 5. Now you can invoke your functions remotely. When a function expects a closure, give it the name
-- of the generated closure, rather than the name of the original function. If the function takes parameters,
-- so will the closure. To start the @greet@ function on @someNode@:
--
--   > spawn someNode (greet__closure "John Baptist")
--
-- Note that we say @greet__closure@ rather than just @greet@. If you prefer, you can use 'mkClosure' instead, i.e. @$(mkClosure 'greet)@, which will expand to @greet__closure@. To calculate a Fibonacci number remotely:
--
-- > val <- callRemotePure someNode (badFib__closure 5)
remotable :: [Name] -> Q [Dec]
remotable names =
    do info <- liftM concat $ mapM getType names 
       loc <- location
       declGen <- mapM (makeDec loc) info
       decs <- sequence $ concat (map fst declGen)
       let outnames = concat $ map snd declGen
       regs <- sequence $ makeReg loc outnames
       return $ decs ++ regs
    where makeReg loc names' = 
              let
                      mkentry = [e| putReg |]
                      regtype = [t| RemoteCallMetaData |]
                      registryName = (mkName "__remoteCallMetaData")
                      reasonableNameModule name = maybe (loc_module loc++".") ((flip (++))".") (nameModule name)
                      app2E op l r = appE (appE op l) r
                      param = mkName "x"
                      applies [] = varE param
                      applies [h] = appE (app2E mkentry (varE h) (litE $ stringL (reasonableNameModule h++nameBase h))) (varE param)
                      applies (h:t) = appE (app2E mkentry (varE h) (litE $ stringL (reasonableNameModule h++nameBase h))) (applies t)
                      bodyq = normalB (applies names')
                      sig = sigD registryName regtype
                      dec = funD registryName [clause [varP param] bodyq []]
               in [sig,dec]
          makeDec loc (aname,atype) =
              do payload <- [t| Payload |]
                 ttprocessm <- [t| ProcessM |]
                 tttaskm <- [t| TaskM |]
                 ttclosure <- [t| Closure |]
                 ttio <- [t| IO |]
                 return $ let
                    implName = mkName (nameBase aname ++ "__impl") 
                    implPlName = mkName (nameBase aname ++ "__implPl")
                    implFqn = loc_module loc ++"." ++ nameBase aname ++ "__impl"
                    closureName = mkName (nameBase aname ++ "__closure")
                    paramnames = map (\x -> 'a' : show x) [1..(length (init arglist))]
                    paramnamesP = (map (varP . mkName) paramnames)
                    paramnamesE = (map (varE . mkName) paramnames)
                    closurearglist = init arglist ++ [processmtoclosure (last arglist)]
                    implarglist = payload : [toPayload (last arglist)]
                    toProcessM a = (AppT ttprocessm a)
                    toPayload x = case funtype of
                                     0 -> case x of
                                           (AppT (ConT n) _) -> (AppT (ConT n) payload)
                                           _ -> toProcessM payload
                                     _ -> toProcessM payload
                    processmtoclosure (AppT mc x) | mc == ttprocessm && isarrow x = AppT ttclosure x
                    processmtoclosure (x) =  (AppT ttclosure x)
                    isarrowful = isarrow $ last arglist
                    isarrow (AppT (AppT ArrowT _) _) = True
                    isarrow (AppT (process) v) 
                        | isarrow v && (process == tttaskm || process == ttprocessm) = True
                    isarrow _ = False
                    applyargs f [] = f
                    applyargs f (l:r) = applyargs (appE f l) r
                    funtype :: Integer
                    funtype = case last arglist of
                                 (AppT (process) _) |  process == ttprocessm -> 0
                                                    |  process == ttio -> 1
                                 _ -> 2
                    just a = conP (mkName "Prelude.Just") [a]
                    errorcall = [e| Prelude.error |]
                    liftio = [e| Control.Monad.Trans.liftIO |]
                    returnf = [e| Prelude.return |]
                    asProcessM x = case funtype of
                                     0 -> x
                                     1 -> case x of
                                            (AppT (ConT _) a) -> AppT ttprocessm a
                                            _ -> AppT ttprocessm x
                                     2 -> AppT ttprocessm x
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
{- UNUSED
                    implPls = if isarrowful then [implPldec,implPldef] else []
-}
                    implPldec = case last arglist of
                                 (AppT ( process) _v) |  process == tttaskm ->
                                      sigD implPlName (return $ putParams $ [payload,(AppT process payload)])
                                 _ -> sigD implPlName (return $ putParams implarglist)
                    implPldef = case last arglist of
                                 (AppT ( process) _) |  process == tttaskm ->
                                      funD implPlName [clause [varP (mkName "a")]
                                                     (normalB (appE [e| remoteCallRectify |] (appE (varE implName) (varE (mkName "a") )))) []
                                           ]
                                 _ -> funD implPlName [clause [varP (mkName "a")]
                                         (normalB (doE [bindS (varP (mkName "res")) ( (appE (varE implName) (varE (mkName "a")))),
                                                       noBindS (appE liftio (appE encodecallio (varE (mkName "res")))) ] )) [] ] 
                    arglist = getParams atype
                  in ([closuredec,closuredef,impldec,impldef]++if not isarrowful then [implPldec,implPldef] else [],
                              [aname,implName]++if not isarrowful then [implPlName] else [])
 
getType :: Name -> Q [(Name, Type)]
getType name = 
  do info <- reify name
     case info of 
       VarI iname itype _ _ -> return [(iname,itype)]
       _ -> return []

putParams :: [Type] -> Type
putParams (afst:lst:[]) = AppT (AppT ArrowT afst) lst
putParams (afst:[]) = afst
putParams (afst:lst) = AppT (AppT ArrowT afst) (putParams lst)
putParams [] = error "Unexpected parameter type in remotable processing"

getParams :: Type -> [Type]
getParams typ = case typ of
                            AppT (AppT ArrowT b) c -> b : getParams c
                            b -> [b]
                                

