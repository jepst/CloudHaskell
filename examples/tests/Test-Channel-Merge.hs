{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

-- This example demonstrates the difference between
-- the biased and round-robin methods for merging
-- channels. Run this program twice, once with
-- the parameter "biased" and once with "rr";
-- the order that the messages will be received
-- in will change, even though the order that they
-- are sent in does not.

import Remote

channelCombiner args = case args of
                         ["biased"] ->  combinePortsBiased
                         ["rr"] -> combinePortsRR
                         _ -> error "Please specify 'biased' or 'rr' on the command line"

initialProcess _ = do
              mypid <- getSelfPid
              args <- getCfgArgs

              (sendchan,recvchan) <- newChannel
              (sendchan2,recvchan2) <- newChannel

              spawnLocal $ mapM_ (sendChannel sendchan) [1..(26::Int)]
              spawnLocal $ mapM_ (sendChannel sendchan2) ['A'..'Z']

              merged <- (channelCombiner args) [combinedChannelAction recvchan show,combinedChannelAction recvchan2 show]
              let go = do item <- receiveChannel merged
                          say $ "Got: " ++ show item
                          go
              go

main = remoteInit (Just "config") [] initialProcess




