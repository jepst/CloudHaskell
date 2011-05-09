{-# LANGUAGE DeriveDataTypeable #-}

-- | A simple type to represent a closure, that is, a function
-- and its environment. The current implementation represents
-- functions as strings, but this could be theoretically
-- swapped out for the \"static\" mechanism described in the
-- paper.
module Remote.Closure (
                       -- * Closure
                       Closure(..)
                       ) where

import Data.Binary (Binary,get,put)
import Data.Typeable (Typeable)
import Remote.Encoding (Payload)

-- | A data type representing a closure, that is, a function with its environment.
--   In spirit, this is actually:
--    
-- >   data Closure a where
-- >     Closure :: Serializable v => (v -#> a) -> v -> Closure a     
--
--   where funny arrow (-#>) identifies a function with no free variables.
--   We simulate this behavior by identifying top-level functions as strings.
--   See the paper for clarification.
data Closure a = Closure String Payload
     deriving (Typeable)

instance Show (Closure a) where
     show a = case a of
                (Closure fn pl) -> show fn

instance Binary (Closure a) where
     get = do s <- get
              v <- get
              return $ Closure s v 
     put (Closure s v) = put s >> put v

