{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote

import Control.Exception (throw)
import Data.Generics (Data)
import Data.Maybe (fromJust)
import Data.Typeable (Typeable)
import Control.Monad (when,forever)
import Control.Monad.Trans (liftIO)
import Data.Char (isUpper)
import Control.Concurrent (threadDelay)
import Data.Binary
import Control.Concurrent.MVar

sayHi :: ProcessId -> ProcessM ()
sayHi s = do liftIO $ threadDelay 500000
             say $ "Hi there, " ++ show s ++ "!"

add :: Int -> Int -> Int
add a b = a + b

badFac :: Integer -> Integer
badFac 0 = 1
badFac 1 = 1
badFac n = badFac (n-1) + badFac (n-2)

remotable ['sayHi, 'add, 'badFac]


while :: (Monad m) => m Bool -> m ()
while a = do f <- a
             when (f)
                (while a >> return ())
             return ()

initialProcess "MASTER" = do
              mypid <- getSelfPid
              mynode <- getSelfNode
              peers <- getPeers

              let slaves = findPeerByRole peers "SLAVE"
              case slaves of
                (somenode:_) -> 
                    do say $ "Running badFac on " ++show somenode
                       res <- callRemotePure somenode (badFac__closure 40)
                       say $ "Got result: " ++ show res
                _ -> say "Couldn't find a SLAVE node to run program on; start a SLAVE, then the MASTER"


              return ()
initialProcess "SLAVE" = receiveWait []
initialProcess _ = liftIO $ putStrLn "Please use parameter -cfgRole=MASTER or -cfgRole=SLAVE"

testSend = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

main = testSend

