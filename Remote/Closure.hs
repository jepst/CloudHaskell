{-# LANGUAGE DeriveDataTypeable #-}
module Remote.Closure (Closure(..)) where

import Data.Binary (Binary,get,put)
import Data.Typeable (Typeable)
import Remote.Encoding (Payload)

data Closure a = Closure String Payload
     deriving (Typeable)
{- 
   In spirit, this is actually:    
>   data Closure a where
>     Closure :: Serializable v => (v -#> a) -> v -> Closure a     
   Where "funny arrow" (-#>) identifies a function with no free variables.
   We simulate this behavior by identifying top-level functions as strings.
-}

instance Show (Closure a) where
     show a = case a of
                (Closure fn pl) -> show fn

instance Binary (Closure a) where
     get = do s <- get
              v <- get
              return $ Closure s v 
     put (Closure s v) = put s >> put v

