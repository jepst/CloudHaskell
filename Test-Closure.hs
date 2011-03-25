{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote.Init
import Remote.Call
import Remote.Process 
import Remote.Peer
import Remote.Encoding
import Remote.Channel

import Control.Exception (throw)
import Data.Generics (Data)
import Data.Maybe (fromJust)
import Data.Typeable (Typeable)
import Control.Monad (when,forever)
--import Control.Monad.Trans (liftIO)
import Data.Char (isUpper)
import Control.Concurrent (threadDelay)
import Data.Binary
import Control.Concurrent.MVar

data Ping = Ping ProcessId deriving (Data,Typeable)
data Pong = Pong ProcessId deriving (Data,Typeable)
instance Binary Ping where put = genericPut; get=genericGet
instance Binary Pong where put = genericPut; get=genericGet

           
sayHi :: String -> ProcessM ()
sayHi s = say s


sayHiPM :: String -> ProcessM String
sayHiPM s = return $ reverse s


sayHiIO :: String -> IO ()
sayHiIO s = putStrLn s

sayHiPure :: Int -> Int
sayHiPure s = s + 1

remotable ['sayHi, 'sayHiIO,'sayHiPure, 'sayHiPM]


while :: (Monad m) => m Bool -> m ()
while a = do f <- a
             when (f)
                (while a >> return ())
             return ()

initialProcess _ = do
              mynid <- getSelfNode
              spawn mynid (sayHi__closure "hi")
              q <- callRemotePure mynid (sayHiPure__closure 1)
              say $ show q
              v <- callRemote mynid (sayHiPM__closure "blarf")
              say $ show v
              w <- callRemoteIO mynid (sayHiIO__closure "fromputstrln")
              say $ show w
              return ()

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

