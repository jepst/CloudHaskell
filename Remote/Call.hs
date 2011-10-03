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
import Data.Maybe (isJust)
import Remote.Closure (Closure(..))
import Remote.Process (ProcessM)
import Remote.Reg (putReg,RemoteCallMetaData)
import Remote.Task (TaskM,serialEncodeA,serialDecodeA)

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
 do e <- makeEnv
    inf <- reify name
    case inf of
       VarI aname atype _ _ -> 
          case nameModule aname of
             Just a -> case a == loc_module (eLoc e) of
                           False -> error "Can't use mkClosureRec across modules: use mkClosure instead"
                           True -> do (aat,aae) <- closureInfo e aname atype
                                      sigE (return aae) (return aat)
             _ -> error "mkClosureRec can't figure out module of symbol"
       _ -> error "mkClosureRec applied to something weird"


closureInfo :: Env -> Name -> Type -> Q (Type,Exp)
closureInfo e named typed = 
  do v <- theval
     return (thetype,v)
   where
     implFqn = loc_module (eLoc e) ++ "." ++ nameBase named ++ "__0__impl"
     (params, returns) = getReturns typed 0
     wrapit x = case isArrowType e x of
                    False -> AppT (eClosureT e) x
                    True -> wrapMonad e (eClosureT e) x
     thetype = putParams (params ++ [wrapit (putParams returns)])
     theval = lamE (map varP paramnames) (appE (appE [e|Closure|] (litE (stringL implFqn))) (appE [e|serialEncodePure|] (tupE (map varE paramnames))))
     paramnames = map (\x -> mkName $ 'a' : show x) [1..(length params)]

closureDecs :: Env -> Name -> Type -> Q [Dec]
closureDecs e n t =
  do (nt,ne) <- closureInfo e n t
     sequence [sigD closureName (return nt),
               funD closureName [clause [] (normalB $ return ne) []]]
  where closureName = mkName $ nameBase n ++ "__closure"

     
data Env = Env
  { eProcessM :: Type
  , eIO :: Type
  , eTaskM :: Type
  , ePayload :: Type
  , eLoc :: Loc
  , eLiftIO :: Exp
  , eReturn :: Exp
  , eClosure :: Exp
  , eClosureT :: Type
  }

makeEnv :: Q Env
makeEnv = 
  do eProcessM <- [t| ProcessM |]
     eIO <- [t| IO |]
     eTaskM <- [t| TaskM |]
     eLoc <- location
     ePayload <- [t| Payload |]
     eLiftIO <- [e|liftIO|]
     eReturn <- [e|return|]
     eClosure <- [e|Closure|]
     eClosureT <- [t|Closure|]
     return Env {
                  eProcessM=eProcessM,
                  eIO = eIO,
                  eTaskM = eTaskM,
                  eLoc = eLoc,
                  ePayload=ePayload,
                  eLiftIO=eLiftIO,
                  eReturn=eReturn,
                  eClosure=eClosure,
                  eClosureT=eClosureT
                }

isMonad :: Env -> Type -> Bool
isMonad e t
  = t == eProcessM e
    || t == eIO e
    || t == eTaskM e

monadOf :: Env -> Type -> Maybe Type
monadOf e (AppT m _) |  isMonad e m = Just m
monadOf e _ = Nothing

restOf :: Env -> Type -> Type
restOf e (AppT m r ) | isMonad e m = r
restOf e r = r

wrapMonad :: Env -> Type -> Type -> Type
wrapMonad e monad val =
  case monadOf e val of
    Just t | t == monad -> val
    Just n -> AppT monad (restOf e val)
    Nothing -> AppT monad val

getReturns :: Type -> Int -> ([Type],[Type])
getReturns t shift = splitAt ((length arglist - 1) - shift) arglist
  where arglist = getParams t

countReturns :: Type -> Int
countReturns t = length $ getParams t

applyArgs :: Exp -> [Exp] -> Exp
applyArgs f [] = f
applyArgs f (l:r) = applyArgs (AppE f l) r

isArrowType :: Env -> Type -> Bool
isArrowType _ (AppT (AppT ArrowT _) _) = True 
isArrowType e t | (isJust $ monadOf e t) && isArrowType e (restOf e t) = True
isArrowType _ _ = False

generateDecl :: Env -> Name -> Type -> Int -> Q [Dec]
generateDecl e name t shift =
   let
     implName = mkName (nameBase name ++ "__" ++ show shift ++ "__impl") 
     implPlName = mkName (nameBase name ++ "__" ++ show shift ++ "__implPl")
     (params,returns) = getReturns t shift
     topmonad = case monadOf e $ last returns of
                 Just p | p == (eTaskM e) -> eTaskM e
                 _ -> eProcessM e
     lifter :: Exp -> ExpQ
     lifter x = case monadOf e $ putParams returns of
                 Just p | p == topmonad -> return x
                 Just p | p == eIO e -> return $ AppE (eLiftIO e) x
                 _ -> return $ AppE (eReturn e) x
     serialEncoder x = case topmonad of
                 p | p == eTaskM e -> appE [e|serialEncodeA|] x
                 _ -> appE [e|liftIO|] (appE [e|serialEncode|] x)
     serialDecoder x = case topmonad of
                 p | p == eTaskM e -> appE [e|serialDecodeA|] x
                 _ -> appE [e|liftIO|] (appE [e|serialDecode|] x)
     paramnames = map (\x -> 'a' : show x) [1..(length params)]
     paramnamesP = (map (varP . mkName) paramnames)
     paramnamesE = (map (VarE . mkName) paramnames)

     just a = conP (mkName "Prelude.Just") [a]

     impldec = sigD implName (appT (appT arrowT (return (ePayload e))) (return $ wrapMonad e topmonad $ putParams returns))
     impldef = funD implName [clause [varP (mkName "a")]
                 (normalB (doE [bindS (varP (mkName "res")) ((serialDecoder (varE (mkName "a")))),
                                noBindS (caseE (varE (mkName "res"))
                                        [match (just (tupP paramnamesP)) (normalB (lifter (applyArgs (VarE name) paramnamesE))) [],
                                         match wildP (normalB (appE [e|error|] (litE (stringL ("Bad decoding in closure splice of "++nameBase name))))) []])
                                      ]))
                      []]
     implPldec = sigD implPlName (return $ putParams $ [ePayload e,wrapMonad e topmonad (ePayload e)] )
     implPldef = funD implPlName [clause [varP (mkName "a")]
                                         (normalB (doE [bindS (varP (mkName "res")) ( (appE (varE implName) (varE (mkName "a")))),
                                                       noBindS ((serialEncoder (varE (mkName "res")))) ] )) [] ] 
     base1 = [impldec,impldef]
     base2 = if isArrowType e $ putParams returns 
                then []
                else [implPldec,implPldef]
  in do cld <- closureDecs e name t
        sequence $ base1++base2++(map return cld)


generateDecls :: Env -> Name -> Q [Dec]
generateDecls e name = 
   do tr <- getType name
      case tr of
        Nothing -> error "remotable applied to bad name"
        Just (fname,ftype) ->
-- Change the following line to: [0..countReturns ftype - 1]
-- to automatically enable partial closure generators
            liftM concat $ mapM (generateDecl e fname ftype) [0]

generateMetaData :: Env -> [Dec] -> Q [Dec]
generateMetaData e decls = sequence [sig,dec]
  where regDecls [] = []
        regDecls (first:rest) =
           case first of
              SigD named _ -> named : (regDecls rest)
              _ -> regDecls rest
        registryName = (mkName "__remoteCallMetaData")
        paramName = mkName "x"
        sig = sigD registryName [t| RemoteCallMetaData |]
        dec = funD registryName [clause [varP paramName] (normalB (toChain (regDecls decls))) []]
        fqn n = (maybe (loc_module (eLoc e)++".") ((flip (++))".") (nameModule n)) ++ nameBase n
        app2E op l r = appE (appE op l) r
        toChain [] = varE paramName
        toChain [h] = appE (app2E [e|putReg|] (varE h) (litE $ stringL (fqn h))) (varE paramName)
        toChain (h:t) = appE (app2E [e|putReg|] (varE h) (litE $ stringL (fqn h))) (toChain t)           

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
   do env <- makeEnv
      newDecls <- liftM concat $ mapM (generateDecls env) names
      lookup <- generateMetaData env newDecls
      return $ newDecls ++ lookup

getType name = 
  do info <- reify name
     case info of 
       VarI iname itype _ _ -> return $ Just (iname,itype)
       _ -> return Nothing

putParams :: [Type] -> Type
putParams (afst:lst:[]) = AppT (AppT ArrowT afst) lst
putParams (afst:[]) = afst
putParams (afst:lst) = AppT (AppT ArrowT afst) (putParams lst)
putParams [] = error "Unexpected parameter type in remotable processing"

getParams :: Type -> [Type]
getParams typ = case typ of
                            AppT (AppT ArrowT b) c -> b : getParams c
                            b -> [b]
                                

