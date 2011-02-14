{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote.Call (remoteCall,registerCalls,empty,Serializable)
import Remote.Process
import Remote.Encoding

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
import Control.Concurrent.Chan

remoteCall [d|
                 myPutStrLn=putStrLn 
           |]


while :: (Monad m) => m Bool -> m ()
while a = do f <- a
             when (f)
                (while a >> return ())
             return ()


receiveWait2 m = do a <- receiveTimeout 1000000 m
                    return $ fromJust a

initialProcess "NODE" = do
              a <- spawn $ while $ receiveWait2 [roundtripResponse PldUser (\x -> if x == "halt" then return ("halted",False) else return (reverse x,True))]

              mapM_ (doit a) ["hi there","spatula","halt","noreturn"]

doit pid s = do
                        res <- roundtripQuery PldUser pid s 
                        liftIO $ print (res :: Either TransmitStatus String)
                        liftIO $ threadDelay 900000
                        send pid ["fuck"]

testSend = do 
          lookup <- registerCalls [Main.__remoteCallMetaData]
          cfg <- readConfig True (Just "config")
          node <- initNode cfg lookup
          startLocalRegistry cfg False
          forkAndListenAndDeliver node cfg PldUser
          roleDispatch node initialProcess
          waitForThreads node
          threadDelay 400000

main = testSend


