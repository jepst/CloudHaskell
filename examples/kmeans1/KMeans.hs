{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where

import Remote.Process
import Remote.Encoding
import Remote.Init
import Remote.Call
import Remote.Peer

import KMeansCommon

import System.Random (randomR,getStdRandom)
import Data.Typeable (Typeable)
import Data.Data (Data)
import Data.Binary (Binary,get,put)
import Data.Maybe (fromJust)
import Data.List (minimumBy,sortBy)
import Data.Either (rights)
import qualified Data.Map as Map
import System.IO
import Debug.Trace

{-
maptask
        reads in all clusters
        and some subset of points
        and for each point, determines which cluster it is nearest

reducetask
        reads in assignments of points to clusters -- all assignments to$
        generates new clusters
        returns if points are converged
-}



split :: Int -> [a] -> [[a]] 
split numChunks l = splitSize (ceiling $ fromIntegral (length l) / fromIntegral numChunks) l
   where
      splitSize _ [] = []
      splitSize i v = take i v : splitSize i (drop i v)

broadcast :: (Serializable a) => [ProcessId] -> a -> ProcessM ()
broadcast pids dat = mapM_ (\pid -> send pid dat) pids

multiSpawn :: [NodeId] -> Closure (ProcessM ()) -> ProcessM [ProcessId]
multiSpawn nodes f = mapM (\node -> spawn node f) nodes

mapperProcess :: ProcessM ()
mapperProcess = 
        let mapProcess :: (Maybe [Vector],Maybe [ProcessId],Map.Map Int [Vector]) -> ProcessM ()
            mapProcess (mvecs,mreducers,mresult) = 
                         receiveWait
                         [
                            match (\vec -> return (Just vec,mreducers,mresult)),
                            match (\reducers -> return (mvecs,Just reducers,mresult)),
                            roundtripResponse PldUser (\() -> return (mresult,(mvecs,mreducers,mresult))),
                            roundtripResponse PldUser
                                  (\clusters -> let tbl = analyze mvecs clusters
                                                    reducers = fromJust mreducers
                                                    target clust = reducers !! (clust `mod` length reducers)
                                                    sendout (clustid,pts) =  send (target clustid) (makeCluster clustid pts)
                                                 in mapM_ sendout (Map.toList tbl)
                                                    >> return ((),(mvecs,mreducers,tbl))),
                            matchUnknownThrow
                         ] >>= mapProcess
            analyze (Just vectors) clusters = 
                   let assignments = map (assignToCluster clusters) vectors
                       changetoLists l = map (\(id,pt) -> (id,[pt])) l
                       toMap l = Map.fromListWith (++) l
                    in toMap (changetoLists assignments)
            assignToCluster clusters vector = 
                   let distances = map (\x -> (clId x,sqDistance (clusterCenter x) vector)) clusters
                    in (fst (minimumBy (\(_,a) (_,b) -> compare a b) distances),vector)
            doit = mapProcess (Nothing,Nothing,Map.empty)
         in doit >> return ()

reducerProcess :: ProcessM ()
reducerProcess = let reduceProcess :: ([Cluster],[Cluster]) -> ProcessM ()
                     reduceProcess (oldclusters,clusters) = 
                          receiveWait [
                                       roundtripResponse PldUser (\() -> return (clusters,(clusters,[]))),
                                       match (\x -> return (oldclusters,combineClusters clusters x)),
                                       matchUnknownThrow] >>= reduceProcess
                     combineClusters :: [Cluster] -> Cluster -> [Cluster]
                     combineClusters [] a = [a]
                     combineClusters (fstclst:rest) clust | clId fstclst == clId clust = (Cluster {clId = clId fstclst,
                                                                                                   clCount = clCount fstclst + clCount clust,
                                                                                                   clSum = addVector (clSum fstclst) (clSum clust)}):rest
                     combineClusters (fstclst:res) clust = fstclst:(combineClusters res clust)
                  in reduceProcess ([],[]) >> return ()

remotable ['mapperProcess, 'reducerProcess]

readableShow [] = []
readableShow ((_,one):rest) = (concat $ map (\(Vector x y) -> show x ++ " " ++ show y ++ "\n") one)++"\n\n"++readableShow rest

initialProcess "MASTER" = 
  do peers <- getPeers
     let mappers = findPeerByRole peers "MAPPER"
     let reducers = findPeerByRole peers "REDUCER"
     points <- liftIO $ getPoints "kmeans-points"
     clusters <- liftIO $ getClusters "kmeans-clusters"
     say $ "Got " ++ show (length points) ++ " points and "++show (length clusters)++" clusters"
     say $ "Got peers: " ++ show peers
     mypid <- getSelfPid
     
     mapperPids <- multiSpawn mappers mapperProcess__closure

     reducerPids <- multiSpawn  reducers reducerProcess__closure
     broadcast mapperPids reducerPids
     mapM_ (\(pid,chunk) -> send pid chunk) (zip (mapperPids) (split (length mapperPids) points))

     let loop howmany clusters = do
           roundtripQueryMulti PldUser mapperPids clusters :: ProcessM [Either TransmitStatus ()]
           res <- roundtripQueryMulti PldUser reducerPids () :: ProcessM [Either TransmitStatus [Cluster]]
           let newclusters = rights res
           let newclusters2 = (sortBy (\a b -> compare (clId a) (clId b)) (concat newclusters))
           if newclusters2 == clusters || howmany >= 5
              then do
                     say $ "Converged in " ++ show howmany ++ " iterations"
                     respoints <- roundtripQueryMulti PldUser mapperPids () :: ProcessM [Either TransmitStatus (Map.Map Int [Vector])]
                     
                     liftIO $ writeFile "kmeans-converged" $ readableShow $ Map.toList $ Map.unionsWith (++) (rights respoints)
              else
                  loop (howmany+1) newclusters2
     loop 0 clusters

initialProcess "MAPPER" = receiveWait []
initialProcess "REDUCER" = receiveWait []
initialProcess _ = error "Role must be MAPPER or REDUCER or MASTER"

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess
