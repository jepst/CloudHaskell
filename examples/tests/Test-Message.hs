{-# LANGUAGE DeriveDataTypeable,TemplateHaskell #-}
module Main where

import Remote

import Data.Typeable (Typeable)
import Data.Data (Data)
import Data.Binary (Binary,get,put)
import System.Random (randomR,getStdRandom)
import Control.Concurrent (threadDelay)
import Control.Monad (when)
import Control.Monad.Trans (liftIO)

data Chunk = Chunk Int deriving (Typeable,Data)

instance Binary Chunk where
    get = genericGet
    put = genericPut

slaveWorker :: ProcessM ()
slaveWorker = 
   receiveWait [match(\sndport -> mapM (sendrand sndport) [0..50] >> sendChannel sndport (Chunk 0))]
  where sendrand sndport _ = 
          do newval <- liftIO $ getStdRandom (randomR (1,100))
             sendChannel sndport (Chunk newval)

$( remotable ['slaveWorker] )

testNode :: ProcessId -> ProcessM ()
testNode pid =
  let getvals mainpid rcv =
        do (Chunk val) <- receiveChannel rcv
           send mainpid val
           when (val /= 0)
              (getvals mainpid rcv)
   in
    do (sendport, receiveport) <- newChannel 
       self <- getSelfPid
       send pid sendport
       spawnLocal $ getvals self receiveport
       return ()
                      

initialProcess "MASTER" =
     do peers <- getPeers
        let slaves = findPeerByRole peers "SLAVE"

        slavepids <- mapM (\slave -> spawn slave slaveWorker__closure) slaves
        mapM_ testNode slavepids

        let getsome = 
             do res <- receiveWait [match(\i -> say ("Got " ++ show (i::Int)) >> return (i/=0))]
                when res getsome
        mapM_ (const getsome) slaves

initialProcess "SLAVE" = receiveWait []

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

