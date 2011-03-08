{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote.Call
import Remote.Process
import Remote.Channel
import Remote.Encoding
import Remote.Peer
import Remote.Init

import Data.Binary (Binary,get,put)
import Data.Char (isUpper)
import Data.Generics (Data)
import Data.Typeable (Typeable)
import Prelude hiding (catch)
import Data.Typeable (typeOf)
import System.IO
import Control.Monad.Trans
import Control.Exception
import Control.Monad
import Data.Maybe (fromJust)
import Control.Concurrent

remoteCall 
  [d|

   stuff :: Int -> String -> [ProcessId] -> ProcessM String
   stuff a b c = return $ show a ++ show b ++ show c

   task :: ProcessM ()
   task = do     mypid <- getSelfPid
                 say $ "I am " ++ show mypid
                 liftIO $ threadDelay 50000
                 receiveWait [match (\chan -> mapM_ (sendChannel chan) (reverse ([1..50]::[Int])))]

   |]

initialProcess "SLAVE" = do
              receiveWait []

initialProcess "MASTER" = do
              mypid <- getSelfPid
  
              (sendchan,recvchan) <- newChannel
              peers <- getPeers
              say ("Got peers: " ++ show peers)
              let slaves = findPeerByRole peers "SLAVE"

              -- The following hack shows the need for an improved "local process registry"
              mapM_ (\x -> do
                              newpid <- spawnRemote x task__closure
                              send newpid sendchan
                              ) slaves
              liftIO $ threadDelay 500000

              mapM_ (\_ -> do v <- receiveChannel recvchan
                              say $ show (v::Int)) [1..50]


main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess




