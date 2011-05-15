{-# LANGUAGE DeriveDataTypeable,CPP,FlexibleInstances,UndecidableInstances #-}

-- | This module provides the 'Serializable' type class and
-- functions to convert to and from 'Payload's. It's implemented
-- in terms of Haskell's "Data.Binary". The message sending
-- and receiving functionality in "Remote.Process" depends on this.
module Remote.Encoding (
          Serializable,
          serialEncode,
          serialEncodePure,
          serialDecode,
          serialDecodePure,
          Payload,
          PayloadLength,
          hPutPayload,
          hGetPayload,
          payloadLength,
          getPayloadType,
          getPayloadContent,
          genericPut,
          genericGet) where

import Data.Binary (Binary,encode,decode,Put,Get,put,get,putWord8,getWord8)
import Control.Monad (liftM)
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as B (hPut,hGet,length)
import Control.Exception (try,evaluate,ErrorCall)
import Data.Int (Int64)
import System.IO (Handle)
import Data.Typeable (typeOf,Typeable)
import Data.Generics (Data,gfoldl,gunfold, toConstr,constrRep,ConstrRep(..),repConstr,extQ,extR,dataTypeOf)

-- | Data that can be sent as a message must implement
-- this class. The class has no functions of its own,
-- but instead simply requires that the type implement
-- both 'Typeable' and 'Binary'. Typeable can usually
-- be derived automatically. Binary requires the put and get
-- functions, which can be easily implemented by hand,
-- or you can use the 'genericGet' and 'genericPut' flavors,
-- which will work automatically for types implementing
-- 'Data'.
class (Binary a,Typeable a) => Serializable a
instance (Binary a,Typeable a) => Serializable a

data Payload = Payload
                { 
                  payloadType :: !ByteString,
                  payloadContent :: !ByteString
                } deriving (Typeable)
type PayloadLength = Int64

instance Binary Payload where
  put pl = put (payloadType pl) >> put (payloadContent pl)
  get = get >>= \a -> get >>= \b -> return $ Payload {payloadType = a,payloadContent=b}

payloadLength :: Payload -> PayloadLength
payloadLength (Payload t c) = B.length t + B.length c

getPayloadContent :: Payload -> ByteString
getPayloadContent = payloadContent

getPayloadType :: Payload -> String
getPayloadType pl = decode $ payloadType pl

hPutPayload :: Handle -> Payload -> IO ()
hPutPayload h (Payload t c) = B.hPut h (encode (B.length t :: PayloadLength)) >>  
                       B.hPut h t >>
                       B.hPut h (encode (B.length c :: PayloadLength)) >>
                       B.hPut h c

hGetPayload :: Handle -> IO Payload
hGetPayload h = do tl <- B.hGet h (fromIntegral baseLen)
                   t <- B.hGet h (fromIntegral (decode tl :: PayloadLength))
                   cl <- B.hGet h (fromIntegral baseLen)
                   c <- B.hGet h (fromIntegral (decode cl :: PayloadLength))
                   return $ Payload {payloadType = t,payloadContent = c}
    where baseLen = B.length (encode (0::PayloadLength))

serialEncodePure :: (Serializable a) => a -> Payload
serialEncodePure a = let encoding = encode a
                      in encoding `seq` Payload {payloadType = encode $ show $ typeOf a,
                                                 payloadContent = encoding}

-- TODO I suspect that we will get better performance for big messages if let this be lazy
-- see also serialDecode
serialEncode :: (Serializable a) => a -> IO Payload
serialEncode a = do encoded <- evaluate $ encode a -- this evaluate is actually necessary, it turns out; it might be better to just use strict ByteStrings
                    return $ Payload {payloadType = encode $ show $ typeOf a,
                                        payloadContent = encoded}


serialDecodePure :: (Serializable a) => Payload -> Maybe a
serialDecodePure a = (\id -> 
                      let pc = payloadContent a
                      in
                        pc `seq`
                        if (decode $! payloadType a) == 
                              show (typeOf $ id undefined)
                          then Just (id $! decode pc)
                          else Nothing ) id


serialDecode :: (Serializable a) => Payload -> IO (Maybe a)
serialDecode a = (\id ->
                      if (decode $ payloadType a) == 
                            show (typeOf $ id undefined)
                         then do
                                 res <- try (evaluate $ decode (payloadContent a)) 
                                    :: (Serializable a) => IO (Either ErrorCall a)
                                 case res of
                                  Left _ -> return $ Nothing
                                  Right v -> return $ Just $ id v
                         else return Nothing ) id


-- | Data types that can be used in messaging must
-- be serializable, which means that they must implement
-- the 'get' and 'put' methods from 'Binary'. If you
-- are too lazy to write these functions yourself,
-- you can delegate responsibility to this function.
-- It's usually sufficient to do something like this:
--
-- > import Data.Data (Data)
-- > import Data.Typeable (Typeable)
-- > import Data.Binary (Binary, get, put)
-- > data MyType = MkMyType Foobar Int [(String, Waddle Baz)]
-- >             | MkSpatula
-- >                  deriving (Data, Typeable)
-- > instance Binary MyType where
-- >    put = genericPut
-- >    get = genericGet
genericPut :: (Data a) => a ->  Put
genericPut = generic `extQ` genericString
   where generic what = fst $ gfoldl 
            (\(before, a_to_b) a -> (before >> genericPut a, a_to_b a))
            (\x -> (serializeConstr (constrRep (toConstr what)), x))
            what
         genericString :: String -> Put
         genericString = put.encode

-- | See 'genericPut'
genericGet :: Data a => Get a
genericGet = generic `extR` genericString
   where generic = (\id -> liftM id $ deserializeConstr $ \constr_rep ->
                   gunfold (\n -> do n' <- n
                                     g' <- genericGet
                                     return $ n' g')
                           (return)
                           (repConstr (dataTypeOf (id undefined)) constr_rep)) id
         genericString :: Get String
         genericString = do q <- get
                            return $ decode q

serializeConstr :: ConstrRep -> Put
serializeConstr (AlgConstr ix)   = putWord8 1 >> put ix
serializeConstr (IntConstr i)    = putWord8 2 >> put i
serializeConstr (FloatConstr r)  = putWord8 3 >> put r
#if __GLASGOW_HASKELL__ >= 611
serializeConstr (CharConstr c)   = putWord8 4 >> put c
#else
serializeConstr (StringConstr c)   = putWord8 4 >> put (head c)
#endif

deserializeConstr :: (ConstrRep -> Get a) -> Get a
deserializeConstr k = 
      do constr_ix <- getWord8
         case constr_ix of
          1 -> get >>= \ix -> k (AlgConstr ix)
          2 -> get >>= \i -> k (IntConstr i)
          3 -> get >>= \r -> k (FloatConstr r)
#if __GLASGOW_HASKELL__ >= 611
          4 -> get >>= \c -> k (CharConstr c)
#else
          4 -> get >>= \c -> k (StringConstr (c:[]))
#endif
