{-# LANGUAGE DeriveDataTypeable #-}
import Remote.Call (remoteCall,registerCalls,empty,Serializable)
import Remote.Process
import Remote.Encoding
import Remote.Channel
import Control.Concurrent
import Control.Exception
import Network hiding (recvFrom,sendTo)
import Network.Socket hiding (send,accept)
import System.IO (hClose)
import Data.Typeable
import Control.Monad.Trans
import Control.Monad
import Remote.Peer
import Debug.Trace

initialProcess "SLAVE" = spawn slaveProcess >> startDiscoveryService
     where slaveProcess = receiveWait [match (\s -> liftIO $ putStrLn ("I received a " ++ s))] >> slaveProcess

initialProcess "MASTER" = do peers <- queryDiscovery 500000
                             liftIO $ print "I found the following SLAVE nodes on this network:"
                             liftIO $ print $ getPeerByRole peers "SLAVE"

initialProcess _ = liftIO $ putStrLn "Please start the test with flag -cfgRole=SLAVE or -cfgRole=MASTER"

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


