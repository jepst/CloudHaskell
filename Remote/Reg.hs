-- | Runtime metadata functions, part of the 
-- RPC mechanism
module Remote.Reg (
         -- * Runtime metadata
         registerCalls,
         Lookup,
         Identifier,
         putReg,
         getEntryByIdent,
         empty,
         RemoteCallMetaData
           ) where

import Data.Dynamic (Dynamic,toDyn,fromDynamic)
import Data.Typeable (Typeable)
import qualified Data.Map as Map (insert,lookup,Map,empty)

----------------------------------------------
-- * Runtime metadata
------------------------------

-- | Data of this type is generated at compile-time
-- by 'remotable' and can be used with 'registerCalls'
-- and 'remoteInit' to create a metadata lookup table, 'Lookup'.
-- The name '__remoteCallMetaData' will be present
-- in any module that uses 'remotable'.
type RemoteCallMetaData = Lookup -> Lookup


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

