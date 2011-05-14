{-# LANGUAGE TemplateHaskell #-}
module Main where	

import Remote

import Data.List (sort)
           
sayHi :: String -> ProcessM ()
sayHi s = say $ "Greetings, " ++ s

sayHiPM :: String -> ProcessM String
sayHiPM s = say ("Hello, "++s) >> 
               return (sort s)

sayHiIO :: String -> IO String
sayHiIO s = putStrLn ("Hello, " ++ s) >>
                return (reverse s)

sayHiPure :: String -> String
sayHiPure s = "Hello, " ++ s

remotable ['sayHi, 'sayHiIO,'sayHiPure, 'sayHiPM]

initialProcess _ = do
              mynid <- getSelfNode

              spawn mynid (sayHi__closure "Zoltan")

              q <- callRemotePure mynid (sayHiPure__closure "Spatula")
              say $ "Got result " ++ show q

              v <- callRemote mynid (sayHiPM__closure "Jaroslav")
              say $ "Got result " ++ v

              w <- callRemoteIO mynid (sayHiIO__closure "Noodle")
              say $ show w

              return ()

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

