{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote.Init
import Remote.Call
import Remote.Process 
import Remote.Peer
import Remote.Encoding
import Remote.Channel

import Control.Exception (throw)
import Data.Generics (Data)
import Data.Maybe (fromJust)
import Data.Typeable (Typeable)
import Control.Monad (when,forever)
--import Control.Monad.Trans (liftIO)
import Data.Char (isUpper)
import Control.Concurrent (threadDelay)
import Data.Binary
import Control.Concurrent.MVar

data Ping = Ping ProcessId deriving (Data,Typeable)
data Pong = Pong ProcessId deriving (Data,Typeable)
instance Binary Ping where put = genericPut; get=genericGet
instance Binary Pong where put = genericPut; get=genericGet

ping :: ProcessM ()
ping = 
   do { self <- getSelfPid 
      ; receiveWait [
          match (\ (Pong partner) -> 
           let response = Ping self
            in send partner response)
       ]
      ; ping }

foo :: ProcessM Int
foo = receiveWait [match return]


           
sayHi :: ProcessId -> ProcessM ()
sayHi s = do liftIO $ threadDelay 500000
             say $ "Hi there, " ++ show s ++ "!"

remotable ['sayHi]


while :: (Monad m) => m Bool -> m ()
while a = do f <- a
             when (f)
                (while a >> return ())
             return ()

initialProcess "MASTER" = do
              mypid <- getSelfPid
              mynode <- getSelfNode
              cfg <- getConfig

              say $ "I am " ++ show mypid ++ " -- " ++ show (cfgArgs cfg)

              peers <- getPeers
              let slaves = findPeerByRole peers "SLAVE"
              say $ "Found these slave nodes: " ++ show slaves

              say "Making slaves say Hi to me"
              mapM_ (\x -> setRemoteNodeLogConfig x (LogConfig LoTrivial (LtForward mynode) LfAll)) slaves
              mapM_ (\x -> do p <- spawn x (sayHi__closure mypid) 
                              monitorProcess mypid p MaMonitor) slaves
              
              let matchMonitor = match (\(ProcessMonitorException aboutwho reason) -> say $ "Got completion messages: " ++ show (aboutwho,reason))
              let waiting i = when (i>0) ((receiveWait [matchMonitor]) >> waiting (i-1))

              say "Waiting for completion messages from slaves..."
              waiting (length slaves)

              say "All slaves done, terminating master..."

              return ()
initialProcess "SLAVE" = do

              receiveWait []

              return ()
initialProcess _ = liftIO $ putStrLn "Role must be MASTER or SLAVE"

testSend = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

main = testSend

