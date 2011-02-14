module Remote.Init (remoteInit) where

import Remote.Peer (startDiscoveryService)
import Remote.Process (pbracket,localRegistryRegisterNode,localRegistryHello,localRegistryUnregisterNode,startGlobalService,startLoggingService,startSpawnerService,ProcessM,readConfig,initNode,startLocalRegistry,forkAndListenAndDeliver,waitForThreads,roleDispatch,Node,runLocalProcess)
import Remote.Call (registerCalls,RemoteCallMetaData)

import System.FilePath (FilePath)
import Control.Concurrent (threadDelay)
import Control.Monad.Trans (liftIO)
import Control.Concurrent.MVar (MVar,takeMVar,putMVar,newEmptyMVar)

startServices :: ProcessM ()
startServices = 
           do
              startGlobalService
              startLoggingService
              startDiscoveryService
              startSpawnerService

dispatchServices :: MVar Node -> IO ()
dispatchServices node = do mv <- newEmptyMVar
                           runLocalProcess node (startServices >> liftIO (putMVar mv ()))
                           takeMVar mv

remoteInit :: FilePath -> [RemoteCallMetaData] -> (String -> ProcessM ()) -> IO ()
remoteInit config metadata f = do
          lookup <- registerCalls metadata
          cfg <- readConfig True (Just config)
              -- TODO sanity-check cfg
          node <- initNode cfg lookup
          startLocalRegistry cfg False
          forkAndListenAndDeliver node cfg
          dispatchServices node
          roleDispatch node userFunction
          waitForThreads node
          threadDelay 500000 -- make configurable, or something
   where userFunction s = pbracket 
                           (localRegistryHello >> localRegistryRegisterNode) 
                           (\_ -> localRegistryUnregisterNode)  -- TODO this is not correct -- it belongs after waitForThreads
                           (\_ -> f s)
