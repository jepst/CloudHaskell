{-# LANGUAGE TemplateHaskell #-}
module Main where

import Remote
import PiCommon
import Data.List (foldl')

worker :: Int -> Int -> TaskM (Int,Int)
worker count offset = 
   let (numin,numout) = countPairs offset count 
    in do tlogS "PI" LoInformation ("Finished mapper from offset "++show offset)
          return (numin,numout)

$( remotable ['worker] )

initialProcess :: String -> ProcessM ()
initialProcess "WORKER" =
  receiveWait []

initialProcess "MASTER" = 
  let
        interval = 1000000
        numworkers = 5
        numberedworkers = take numworkers [0,interval..]
        clos = map (worker__closure interval) numberedworkers
  in runTask $ 
        do proms <- mapM newPromise clos
           res <- mapM readPromise proms
           let (sumx,sumy) = foldl' (\(a,b) (c,d) -> (a+c,b+d)) (0,0) res
           let (num,den) = estimatePi (fromIntegral sumx) (fromIntegral sumy)
           tsay ("Done: " ++ longdiv (num) (den) 20)
  where 
    estimatePi ni no | ni + no == 0 = (0,0)
                     | otherwise = (4 * ni , ni+no)

initialProcess _ = error "Role must be WORKER or MASTER"

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess


