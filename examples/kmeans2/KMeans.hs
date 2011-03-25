{-# LANGUAGE TemplateHaskell,DeriveDataTypeable,BangPatterns #-}
module Main where

import Remote.Process
import Remote.Encoding
import Remote.Init
import Remote.Call
import Remote.Peer

import KMeansCommon

import Control.Exception (try,SomeException,evaluate)
import Control.Monad (liftM)
import System.Random (randomR,getStdRandom)
import Data.Typeable (Typeable)
import Data.Data (Data)
import Control.Exception (IOException)
import Data.Binary (Binary,get,put,encode,decode)
import Data.Maybe (fromJust)
import Data.List (minimumBy,sortBy)
import Data.Time
import Data.Either (rights)
import qualified Data.ByteString.Lazy as B
import qualified Data.Map as Map
import System.IO
import Debug.Trace

split :: Int -> [a] -> [[a]] 
split numChunks l = splitSize (ceiling $ fromIntegral (length l) / fromIntegral numChunks) l
   where
      splitSize i v = let (first,second) = splitAt i v 
                       in first : splitSize i second

broadcast :: (Serializable a) => [ProcessId] -> a -> ProcessM ()
broadcast pids dat = mapM_ (\pid -> send pid dat) pids

multiSpawn :: [NodeId] -> Closure (ProcessM ()) -> ProcessM [ProcessId]
multiSpawn nodes f = mapM (\node -> spawnLink node f) nodes
   where s n = do mypid <- getSelfNode
                  setRemoteNodeLogConfig n (LogConfig LoTrivial (LtForward mypid) LfAll)
                  spawnLink n f

mapperProcess :: ProcessM ()
mapperProcess = 
        let mapProcess :: (Maybe [Vector],Maybe [ProcessId],Map.Map Int (Int,Vector)) -> ProcessM ()
            mapProcess (mvecs,mreducers,mresult) = 
                         receiveWait
                         [
                            match (\vec -> do vecs<-liftIO $ readf vec
                                              say $ "Mapper read data file" 
                                              return (Just vecs,mreducers,mresult)),
                            match (\reducers -> return (mvecs,Just reducers,mresult)),
                            roundtripResponse (\() -> return (mresult,(mvecs,mreducers,mresult))),
                            roundtripResponse
                                  (\clusters -> let tbl = analyze (fromJust mvecs) clusters Map.empty
                                                    reducers = fromJust mreducers
                                                    target clust = reducers !! (clust `mod` length reducers)
                                                    sendout (clustid,(count,sum)) =  send (target clustid) Cluster {clId = clustid,clCount=count, clSum=sum}
                                                 in do say $ "calculating: "++show (length reducers)++" reducers"
                                                       mapM_ sendout (Map.toList tbl)
                                                       return ((),(mvecs,mreducers,tbl))),
                            matchUnknownThrow
                         ] >>= mapProcess
            getit :: Handle -> IO [Vector]
            getit h = do l <- liftM lines $ hGetContents h
                         return (map read l) -- evaluate or return?
            readf fn = do h <- openFile fn ReadMode 
                          getit h
            condtrace cond s val = if cond
                                      then trace s val
                                      else val
            analyze :: [Vector] -> [Cluster] -> Map.Map Int (Int,Vector) -> Map.Map Int (Int,Vector) 
            analyze [] _ ht = ht
            analyze (v:vectors) clusters ht =
                   let theclust = assignToCluster clusters v
                       newh = ht `seq` theclust `seq` Map.insertWith' (\(a,v1) (b,v2) -> let av = addVector v1 v2 in av `seq` (a+b,av) ) theclust (1,v) ht
-- condtrace (blarg `mod` 1000 == 0) (show blarg) $ 
                    in newh `seq` analyze vectors clusters newh 
            assignToCluster :: [Cluster] -> Vector -> Int
            assignToCluster clusters vector = 
                   let distances = map (\x -> (clId x,sqDistance (clusterCenter x) vector)) clusters
                    in fst $ minimumBy (\(_,a) (_,b) -> compare a b) distances
            doit = mapProcess (Nothing,Nothing,Map.empty)
         in doit >> return ()

reducerProcess :: ProcessM ()
reducerProcess = let reduceProcess :: ([Cluster],[Cluster]) -> ProcessM ()
                     reduceProcess (oldclusters,clusters) = 
                          receiveWait [
                                       roundtripResponse (\() -> return (clusters,(clusters,[]))),
                                       match (\x -> return (oldclusters,combineClusters clusters x)),
                                       matchUnknownThrow] >>= reduceProcess
                     combineClusters :: [Cluster] -> Cluster -> [Cluster]
                     combineClusters [] a = [a]
                     combineClusters (fstclst:rest) clust | clId fstclst == clId clust = (Cluster {clId = clId fstclst,
                                                                                                   clCount = clCount fstclst + clCount clust,
                                                                                                   clSum = addVector (clSum fstclst) (clSum clust)}):rest
                     combineClusters (fstclst:res) clust = fstclst:(combineClusters res clust)
                  in reduceProcess ([],[]) >> return ()


$( remotable ['mapperProcess, 'reducerProcess] )


initialProcess "MASTER" = 
  do peers <- getPeers
--     say $ "Got peers: " ++ show peers
     cfg <- getConfig
     let mappers = findPeerByRole peers "MAPPER"
     let reducers = findPeerByRole peers "REDUCER"
     let numreducers = length reducers
     let nummappers = length mappers
     say $ "Got " ++ show nummappers ++ " mappers and " ++ show numreducers ++ " reducers"
     clusters <- liftIO $ getClusters "kmeans-clusters"
     say $ "Got "++show (length clusters)++" clusters"
     mypid <- getSelfPid
     
     mapperPids <- multiSpawn mappers mapperProcess__closure

     reducerPids <- multiSpawn  reducers reducerProcess__closure
     broadcast mapperPids reducerPids
     mapM_ (\(pid,chunk) -> send pid chunk) (zip (mapperPids) (repeat "kmeans-points"))

     say "Starting iteration"
     starttime <- liftIO $ getCurrentTime
     let loop howmany clusters = do
           liftIO $ putStrLn $ show howmany
           roundtripQueryMulti PldUser mapperPids clusters :: ProcessM [Either TransmitStatus ()]
           res <- roundtripQueryMulti PldUser reducerPids () :: ProcessM [Either TransmitStatus [Cluster]]
           let newclusters = rights res
           let newclusters2 = (sortBy (\a b -> compare (clId a) (clId b)) (concat newclusters))
           if newclusters2 == clusters || howmany >= 4
              then do
                     donetime <- liftIO $ getCurrentTime
                     say $ "Converged in " ++ show howmany ++ " iterations and " ++ (show $ diffUTCTime donetime starttime)
                     pointmaps <- mapM (\pid -> do (Right m) <- roundtripQuery PldUser pid ()
                                                   return (m::Map.Map Int (Int,Vector))) mapperPids
                     let pointmap = map (\x -> sum $ map fst (Map.elems x)) pointmaps
                     say $ "Total points: " ++ (show $ sum pointmap)
--                     liftIO $ writeFile "kmeans-converged" $ readableShow (Map.toList pointmap)
                     --respoints <- roundtripQueryAsync PldUser mapperPids () :: ProcessM [Either TransmitStatus (Map.Map Int [Vector])]
                     
                     --liftIO $ B.writeFile "kmeans-converged" $ encode $ Map.toList $ Map.unionsWith (++) (rights respoints)
              else
                  loop (howmany+1) newclusters2
     loop 0 clusters

initialProcess "MAPPER" = receiveWait []
initialProcess "REDUCER" = receiveWait []
initialProcess _ = error "Role must be MAPPER or REDUCER or MASTER"

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess
