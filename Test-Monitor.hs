{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where	

import Remote.Init
import Remote.Call
import Remote.Process 
import Remote.Peer
import Remote.Encoding

import Control.Exception (throw)
import Data.Generics (Data)
import Data.Maybe (fromJust)
import Data.Typeable (Typeable)
import Control.Monad (when,forever)
import Control.Monad.Trans (liftIO)
import Data.Char (isUpper)
import Control.Concurrent (threadDelay)
import Data.Binary

sayHi :: ProcessM ()
sayHi = receiveWait [match (\(thepid,s) ->
                             do --liftIO $ putChar $ head "" 
                                send thepid (reverse s :: String)
                                liftIO $ threadDelay 5000000
                                liftIO $ putChar $ head "" ),
                                roundtripResponse (\q -> liftIO (threadDelay 9000000 ) >> return (q::String,()))]

$( remotable [ 'sayHi ] )


initialProcess "MASTER" = do
              mypid <- getSelfPid
              mynode <- getSelfNode
              cfg <- getConfig
              peers <- getPeers
              let nid = head $ findPeerByRole peers "NODE"

--              roundtripQuery PldUser nullPid "blorg" :: ProcessM (Either TransmitStatus String)

              pid <- spawn nid sayHi__closure

{-              res <- roundtripQueryUnsafe PldUser pid "yo"
              case res of
                Left a -> say $ show a
                Right b -> say b-}

--              monitorProcess mypid pid MaMonitor
              
              withMonitor pid $ 
                 do send pid (mypid,"hi")
                    receiveWait [match (\s -> say s),
                                 matchProcessDown pid (say "----Down!")]

              return ()
initialProcess _ = receiveWait []

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

