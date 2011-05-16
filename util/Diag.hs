module Main (main) where

-- This is a diagnostic program that will
-- give you feedback about what it sees
-- as your current configuration of Cloud Haskell.
-- If your program isn't able to talk to remote
-- nodes, run this program to check your configuration.

import Remote (remoteInit, getPeers, getSelfNode, ProcessM)
import Remote.Process (hostFromNid,getConfig,cfgNetworkMagic,cfgKnownHosts,cfgPeerDiscoveryPort)

import qualified Data.Map as Map (elems)
import Control.Monad.Trans (liftIO)
import Data.List (intercalate,nub)

s :: String -> ProcessM ()
s = liftIO . putStrLn

orNone :: [String] -> String
orNone [] = "None"
orNone a = intercalate "," a

initialProcess myRole =
   do s "Cloud Haskell diagnostics\n"
      mynid <- getSelfNode
      peers <- getPeers
      cfg <- getConfig
      s $ "I seem to be running on host \""++hostFromNid mynid++"\".\nIf that's wrong, set it using the cfgHostName option.\n"
      s $ "My role is \""++myRole++"\".\nIf that's wrong, set it using the cfgRole option.\n"
      s $ "My magic is \""++cfgNetworkMagic cfg++"\".\nIf that's wrong, set it using the cfgNetworkMagic option.\n"
      s $ "I will look for nodes on the following hosts: " ++ orNone (cfgKnownHosts cfg)
      s $ if cfgPeerDiscoveryPort cfg > 0 
           then "I will also look for nodes on the local network." 
           else "I will not look for nodes on the local network other than those named above."
      let hosts = orNone $ nub $ map (hostFromNid) (concat $ Map.elems peers)
      s $ "I have found nodes on the following hosts: "++hosts++".\nIf I'm not finding all the nodes you expected, make sure they:"
      s $ "\tare running\n\tare not behind a firewall\n\thave the same magic\n\tare listed in cfgKnownHosts"

main = remoteInit (Just "config") [] initialProcess

