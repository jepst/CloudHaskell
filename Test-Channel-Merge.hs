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

channelCombiner cfg = case cfgArgs cfg of
                         ["biased"] ->  combineChannelsBiased
                         ["rr"] -> combineChannelsRR
                         _ -> error "Please specify 'biased' or 'rr' on the command line"

initialProcess "NODE" = do
              mypid <- getSelfPid
              cfg <- getConfig  

              (sendchan,recvchan) <- makeChannel
              (sendchan2,recvchan2) <- makeChannel

              spawn $ mapM_ (sendChannel sendchan) [1..(26::Int)]
              spawn $ mapM_ (sendChannel sendchan2) ['A'..'Z']

              merged <- (channelCombiner cfg) [combinedChannelAction recvchan show,combinedChannelAction recvchan2 show]
              let go = do item <- receiveChannel merged
                          say $ "Got: " ++ show item
                          go
              go

main = remoteInit "config" [Main.__remoteCallMetaData] initialProcess




