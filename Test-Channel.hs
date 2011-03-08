{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote.Call (remoteCall,registerCalls,empty)
import Remote.Process
import Remote.Encoding
import Remote.Channel

import Data.Binary (Binary,get,put)
import Data.Char (isUpper)
import Data.Generics (Data)
import Data.Typeable (Typeable)
import Prelude hiding (catch)
import Data.Typeable (typeOf)
import Control.Monad.Trans
import Control.Exception
import Control.Monad
import Data.Maybe (fromJust)
import Control.Concurrent

initialProcess "NODE" = do
              startGlobalService

              (sendchan,recvchan) <- newChannel

              a <- spawn $ do
                              sendChannel sendchan "hi"
                              sendChannel sendchan "lumpy"
                              liftIO $ threadDelay 1000000
                              sendChannel sendchan "spatula"
                              sendChannel sendchan "noodle"
                              mapM_ (sendChannel sendchan) (map show [1..1000])
              liftIO $ threadDelay 500000
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print
              receiveChannel recvchan >>= liftIO . print



testSend = do 
          lookup <- return empty
          cfg <- readConfig True (Just "config")
          node <- initNode cfg lookup
          startLocalRegistry cfg False
          forkAndListenAndDeliver node cfg
          roleDispatch node initialProcess
          waitForThreads node
          threadDelay 500000

main = testSend


