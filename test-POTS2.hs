{-# LANGUAGE TemplateHaskell #-}
import Remote.Process
import Remote.Call (registerCalls,remoteCall)
import Remote.Encoding (genericGet, genericPut)

import Control.Monad (when)
import Control.Monad.Trans (liftIO)
import Control.Concurrent (threadDelay)

import POTS.StateMachine
import POTS.FakeTeleOS

remoteCall [d|
                f::Int
                f=0
           |]
initializeAndRun proc = do
          lookup <- registerCalls [__remoteCallMetaData]
          cfg <- readConfig True (Just "config")
          node <- initNode cfg lookup
          startLocalRegistry cfg False
          forkAndListenAndDeliver node cfg PldUser
          roleDispatch node proc
          waitForThreads node
          threadDelay 500000

-- main = initializeAndRun initialProcess

main = initializeAndRun (createControlProcs)

{-
Started DialTone on 479708
Stopped tone on 479708
Started BusyTone on 479708
-}
createControlProcsBusy "NODE" = do
	ph1 <- spawn (idle "479708")
	registerProcessName "479708" ph1
	ph2 <- spawn (idle "478637")
	registerProcessName "478637" ph2
	let telephoneCmds = TmOffHook: fmap TmDigitDialed "479708" 
        mapM_ (send ph1) telephoneCmds
	

createControlProcs "NODEA" = do

	ph1 <- spawn (idle "479708")
	registerProcessName "479708" ph1

        me <- getSelfPid
        registerProcessName "NODEA" me
        receiveWait [matchIf ((==)"ok") (const (return ()))]

	let telephoneCmds = TmOffHook: fmap TmDigitDialed "478637" 
        mapM_ (send ph1) telephoneCmds


createControlProcs "NODEB" = do
	ph2 <- spawn (idle "478637")
	registerProcessName "478637" ph2

        let trylookup = do
                  nodeA <- lookupProcessName "NODEA"
                  if nodeA == nullPid
                     then do liftIO $ threadDelay 50000
                             trylookup
                     else return nodeA
        nodeA <- trylookup
        send nodeA "ok"

	liftIO $ threadDelay 500000
        send ph2 TmOffHook
        liftIO $ threadDelay 900000
        send ph2 TmOnHook
