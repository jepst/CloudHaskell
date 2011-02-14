{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote.Init
import Remote.Call
import Remote.Process 
import Remote.Peer

import Data.Generics (Data)
import Data.Maybe (fromJust)
import Data.Typeable (Typeable)
import Control.Monad (when,forever)
--import Control.Monad.Trans (liftIO)
import Data.Char (isUpper)
import Control.Concurrent (threadDelay)

remoteCall [d|

           
           sayHi :: ProcessId -> ProcessM ()
           sayHi s = do liftIO $ threadDelay 500000
                        say $ "Hi there, " ++ show s ++ "!"

           shiftLetter :: Int -> Char -> Char
           shiftLetter n c = let alphabet = if isUpper c then ['A'..'Z'] else ['a'..'z']
                                 letters = cycle alphabet
                                 sletters = take (length alphabet) $ drop n letters
                              in maybe c id (lookup c (zip letters sletters))
 

           rot13 :: String -> String
           rot13 = map (shiftLetter 13)
	|]

while :: (Monad m) => m Bool -> m ()
while a = do f <- a
             when (f)
                (while a >> return ())
             return ()

initialProcess "MASTER" = do
              mypid <- getSelfPid
              mynode <- getSelfNode
              cfg <- getConfig

              say $ "I am " ++ show mypid ++ " -- " ++ show (cfgArgs cfg)

              peers <- getPeersStatic
              let slaves = findPeerByRole peers "SLAVE"
              say $ "Found these slave nodes: " ++ show slaves

              say "Making slaves say Hi to me"
              mapM_ (\x -> setRemoteNodeLogConfig x (LogConfig LoTrivial (LtForward mynode) LfAll)) slaves
              mapM_ (\x -> spawnRemoteAnd x (sayHi__closure mypid) (AmSpawnMonitor (mypid,SigProcessDown,MaMonitor))) slaves
              
              let matchMonitor = match (\(ProcessMonitorMessage aboutwho typ reason) -> say $ "Got completion messages: " ++ show (aboutwho,typ,reason))
              let waiting i = when (i>0) ((receiveWait [matchMonitor]) >> waiting (i-1))

              say "Waiting for completion messages from slaves..."
              waiting (length slaves)

              say "All slaves done, terminating master..."

              return ()
initialProcess "SLAVE" = do

              receiveWait []

              return ()
initialProcess _ = liftIO $ putStrLn "Role must be MASTER or SLAVE"

testSend = remoteInit "config" [Main.__remoteCallMetaData] initialProcess

main = testSend

