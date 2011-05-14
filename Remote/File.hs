module Main where

import Network.BSD (HostEntry(..),getHostName)
import Network (HostName,PortID(..),PortNumber(..),listenOn,accept,sClose,connectTo,Socket)
import Network.Socket (PortNumber(..),setSocketOption,SocketOption(..),socketPort,aNY_PORT )
import System.Timeout (timeout)
import Control.Exception (finally)
import System.IO (hPutStr,hClose,hGetContents)

import Remote.Process (PortId)

data Store a = PromiseFile ProcessId

{-

newPromiseStream :: Closure (PromiseStreamReader a -> TaskM b) -> TaskM (PromiseStreamWriter a)
writePromiseStream :: PromiseStreamWriter a -> a -> TaskM ()
finalizePromiseStream :: PromiseStreamWrier a -> TaskM ()
readPromiseStream :: PromiseStreamReader a -> TaskM (Maybe a)

--------
newtype Raw = Raw String
instance Read Raw where
  readsPrec _ x = [(x,[])]

data Promise a = PromiseBasic { psRedeemer :: ProcessId, psId :: PromiseId } 
               | PromiseImmediate a deriving Typeable
               | PromiseStream ProcessId StreamId

data StreamInfo FilePath ByteRange

Map StreamId StreamInfo

toStream :: FilePath -> Int -> ProcessM [Stream a] -- partioning of input data
readPromise :: Promise a -> TaskM a
writeStream :: SteamWriter a -> a -> TaskM ()
newStream :: (Closure (StreamWriter a -> TaskM ())) -> TaskM (Stream a)
-}

-- The real question is: how to deal with complaints?
-- we should be able to FORCE DISTRIBUTION should certain nodes (where they can later be picked out of the cache)
-- in addition, the master might keep track of locations where copies are available
-- finally, PARTITIONING 

sendFile :: FilePath -> PortId -> Int -> MVar PortId -> IO Bool
sendFile fp portid delay mrealport = 
   do sock <- listenOn whichPort
      setSocketOption sock KeepAlive 1
      realPort <- socketPort sock
      putMVar mrealport (fromEnum realPort)
      goSock sock `finally` sClose sock
  where
     goSock sock =
       do 
          res <- timeoutq delay $ accept sock
             case res of
               Nothing -> return False
               Just (h,_,_) ->
                 let acceptgo =
                     do file <- readFile fp
                        hPutStr h file
                        hClose h
                        return True
                  in acceptgo `finally` hClose h
      timeoutq d f = case d of
                       0 -> do r <- f
                               return $ Just r
                      _ -> timeout d f                     
      whichPort = if portid /= 0
                     then PortNumber $ toEnum $ portid
                     else PortNumber aNY_PORT

receiveFile :: FilePath -> HostName -> PortId -> IO ()
receiveFile fp hostname portid =
  do h <- connectTo hostname (PortNumber $ toEnum portid)
     (do contents <- hGetContents h
         writeFile fp contents) `finally` (hClose h)

