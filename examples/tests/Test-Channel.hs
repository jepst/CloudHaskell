{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote

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

              (sendchan,recvchan) <- newChannel

              a <- spawnLocal $ do
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



main = remoteInit (Just "config") [] initialProcess


