{-# LANGUAGE TemplateHaskell #-}
module Main where

import Remote
import PiCommon

worker :: Int -> Int -> ProcessId -> ProcessM ()
worker count offset master = 
   let (numin,numout) = countPairs offset count 
    in do send master (numin,numout)
          logS "PI" LoInformation ("Finished mapper from offset "++show offset)

$( remotable ['worker] )

initialProcess :: String -> ProcessM ()
initialProcess "WORKER" =
  receiveWait []

initialProcess "MASTER" = 
  do { peers <- getPeers
     ; mypid <- getSelfPid
     ; let { workers = findPeerByRole peers "WORKER"
     ;       interval = 1000000
     ;       numberedworkers = (zip [0,interval..] workers) }
     ; mapM_ (\ (offset,nid) -> spawn nid (worker__closure (interval-1) offset mypid)) numberedworkers
     ; (x,y) <- receiveLoop (0,0) (length workers)
     ; let est = estimatePi (fromIntegral x) (fromIntegral y)
        in say ("Done: " ++ longdiv (fst est) (snd est) 20) }
  where 
    estimatePi ni no | ni + no == 0 = (0,0)
                     | otherwise = (4 * ni , ni+no)
    receiveLoop a 0 = return a
    receiveLoop (numIn,numOut) n = 
      let 
        resultMatch = match (\ (x,y) -> return (x::Int,y::Int))
      in do { (newin,newout) <- receiveWait [resultMatch]
            ; let { x = numIn + newin
            ;       y = numOut + newout }
            ; receiveLoop (x,y) (n-1) }

initialProcess _ = error "Role must be WORKER or MASTER"

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess


