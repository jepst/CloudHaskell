module Main (main) where

import Remote (remoteInit, getPeers, getSelfNode, ProcessM)
import Remote.Process (hostFromNid,getConfig,cfgNetworkMagic,cfgKnownHosts)

import qualified Data.Map as Map (elems)
import Control.Monad.Trans (liftIO)
import Data.List (intercalate)

s :: String -> ProcessM ()
s = liftIO . putStrLn

initialProcess myRole =
   do s "Cloud Haskell diagnostics\n"
      mynid <- getSelfNode
      peers <- getPeers
      cfg <- getConfig
      s $ "I seem to be running on host \""++hostFromNid mynid++"\".\nIf that's wrong, set it using the cfgHostName option.\n"
      s $ "My role is \""++myRole++"\".\nIf that's wrong, set it using the cfgRole option.\n"
      s $ "My magic is \""++cfgNetworkMagic cfg++"\".\nIf that's wrong, set it using the cfgNetworkMagic option.\n"
      s $ "I will look for nodes on the following hosts,\n  as well as any hosts on the local network: " ++ intercalate "," (cfgKnownHosts cfg)
      let hosts = intercalate ", " $ map (hostFromNid) (concat $ Map.elems peers)
      s $ "I have found nodes on the following hosts: "++hosts++".\nIf I'm not finding all the nodes you expected, make sure they:"
      s $ "\tare running\n\tare not behind a firewall\n\thave the same magic\n\tare listed in cfgKnownHosts"

main = remoteInit (Just "config") [] initialProcess

