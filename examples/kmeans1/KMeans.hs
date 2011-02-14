{-# LANGUAGE TemplateHaskell,DeriveDataTypeable #-}
module Main where

import Remote.Process
import Remote.Encoding
import Remote.Init
import Remote.Call
import Remote.Peer

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



data Vector = Vector Double Double deriving (Show,Read,Typeable,Data,Eq)
instance Binary Vector where put = genericPut; get = genericGet

data Cluster = Cluster
               {
                  clId :: Int,
                  clCount :: Int,
                  clSum :: Vector
               } deriving (Show,Read,Typeable,Data,Eq)
instance Binary Cluster where put = genericPut; get = genericGet

clusterCenter :: Cluster -> Vector
clusterCenter cl = let (Vector a b) = clSum cl
                    in Vector (op a) (op b)
       where op = (\d -> d / (fromIntegral $ clCount cl)) 

sqDistance :: Vector -> Vector -> Double
sqDistance (Vector x1 y1) (Vector x2 y2) = (x1-x2)*(x1-x2) + (y1-y2)*(y1-y2)


getPoints :: FilePath -> IO [Vector]
getPoints fp = do c <- readFile fp
                  return $ read c

getClusters :: FilePath -> IO [Cluster]
getClusters fp = do c <- readFile fp
                    return $ read c

split :: Int -> [a] -> [[a]] 
split numChunks l = splitSize (length l `div` numChunks) l -- this might leave some off
   where
      splitSize _ [] = []
      splitSize i v = take i v : splitSize i (drop i v)

broadcast :: (Serializable a) => [ProcessId] -> a -> ProcessM ()
broadcast pids dat = mapM_ (\pid -> send pid dat) pids

multiSpawn :: [NodeId] -> Closure (ProcessM ()) -> ProcessM [ProcessId]
multiSpawn nodes f = mapM (\node -> spawnRemote node f) nodes

mapperProcess :: ProcessM ()
mapperProcess = 
        let mapProcess :: (Maybe [Vector],Maybe [ProcessId]) -> 
                          ProcessM (Maybe [Vector],Maybe [ProcessId])
            mapProcess (mvecs,mreducers) = 
                         receiveWait
                         [
                            match (\vec -> return (Just vec,mreducers)),
                            match (\reducers -> return (mvecs,Just reducers)),
                            roundtripResponse PldUser
                                  (\clusters -> let tbl = analyze mvecs clusters
                                                    reducers = fromJust mreducers
                                                    target clust = reducers !! (clust `mod` length reducers)
                                                    sendout (clustid,pts) =  send (target clustid) (Map.singleton clustid pts)
                                                 in mapM_ sendout (Map.toList tbl)
                                                    >> return ((),(mvecs,mreducers))),
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
            doit = mapProcess (Nothing,Nothing)
         in doit >> return ()

reducerProcess :: ProcessM ()
reducerProcess = let reduceProcess :: (Maybe [ProcessId],Map.Map Int [Vector],Map.Map Int [Vector]) -> ProcessM (Maybe [ProcessId],Map.Map Int [Vector],Map.Map Int [Vector])
                     reduceProcess (mmappers,oldclusters,clusters) = 
                          receiveWait [match (\mappers -> return (Just mappers,oldclusters,clusters)),
                                       roundtripResponse PldUser (\"" -> return (oldclusters,(mmappers,clusters,Map.empty))),
                                       roundtripResponse PldUser (\() -> 
                                                      let cl = toClusters (Map.toList clusters)
                                                          (Just mappers) = mmappers
                                                      in return (cl,(mmappers,clusters,Map.empty))),
                                       match (\x -> return (mmappers,oldclusters,Map.unionWith (++) clusters x)),
                                       matchUnknownThrow] >>= reduceProcess
                     toClusters cls = map (\(clsid,v) -> Cluster {clId=clsid,clCount=length v,clSum = addup v}) cls
                     addup v = let (a,b) = foldl (\(x1,y1) (Vector x2 y2) -> (x1+x2,y1+y2)) (0,0) v
                                in Vector a b
                  in reduceProcess (Nothing,Map.empty,Map.empty) >> return ()

$(remoteCall [d|
   mapperProcessRemote :: ProcessM ()
   mapperProcessRemote = mapperProcess

   reducerProcessRemote :: ProcessM ()
   reducerProcessRemote = reducerProcess
   |] )

readableShow [] = []
readableShow ((_,one):rest) = (concat $ map (\(Vector x y) -> show x ++ " " ++ show y ++ "\n") one)++"\n\n"++readableShow rest

initialProcess "MASTER" = 
  do peers <- getPeersDynamic 50000
     let mappers = findPeerByRole peers "MAPPER"
     let reducers = findPeerByRole peers "REDUCER"
     let numreducers = length reducers
     let nummappers = length mappers
     points <- liftIO $ getPoints "kmeans-points"
     clusters <- liftIO $ getClusters "kmeans-clusters"
     mypid <- getSelfPid
     
     mapperPids <- multiSpawn mappers mapperProcessRemote__closure

     reducerPids <- multiSpawn  reducers reducerProcessRemote__closure
     broadcast mapperPids reducerPids
     broadcast reducerPids mapperPids
     mapM_ (\(pid,chunk) -> send pid chunk) (zip (mapperPids) (split (length mapperPids) points))

     let loop howmany clusters = do
           roundtripQueryAsync PldUser mapperPids clusters :: ProcessM [Either TransmitStatus ()]
           res <- roundtripQueryAsync PldUser reducerPids () :: ProcessM [Either TransmitStatus [Cluster]]
           let newclusters = rights res
           let newclusters2 = (sortBy (\a b -> compare (clId a) (clId b)) (concat newclusters))
           if newclusters2 == clusters
              then do
                     liftIO $ putStrLn $ "------------------Converged in " ++ show howmany ++ " iterations"
                     liftIO $ print $ newclusters2
                     pointmaps <- mapM (\pid -> do (Right m) <- roundtripQuery PldUser pid ""
                                                   return (m::Map.Map Int [Vector])) reducerPids
                     let pointmap = Map.unionsWith (++) pointmaps
                     liftIO $ writeFile "kmeans-converged" $ readableShow (Map.toList pointmap)
              else
                  loop (howmany+1) newclusters2
     loop 0 clusters

initialProcess "MAPPER" = receiveWait []
initialProcess "REDUCER" = receiveWait []
initialProcess _ = error "Role must be MAPPER or REDUCER or MASTER"

makeData :: Int -> Int -> IO ()
makeData n k = 
           let getrand = getStdRandom (randomR (1,1000))
               randvect = do a <- getrand
                             b <- getrand
                             return $ Vector a b
               vects :: IO [Vector]
               vects = sequence (replicate n randvect)
               clusts :: IO [Cluster]
               clusts = mapM (\clid -> do v <- randvect
                                          return $ Cluster {clId = clid,clCount=1,clSum=v}) [1..k]
           in do v <- vects
                 c <- clusts
                 writeFile "kmeans-points" (show v)
                 writeFile "kmeans-clusters" (show c)


makeMouse :: IO ()
makeMouse = let getrand a b = getStdRandom (randomR (a,b))
                randvectr (x,y) r = do rr <- (getrand 0 r)
                                       ra <- (getrand 0 (2*3.1415))
                                       let xo = cos ra * rr
                                       let yo = sin ra * rr
                                       return $ Vector  (x+xo) (y+yo)
                randvect (xmin,xmax) (ymin,ymax) = do x <- (getrand xmin xmax)
                                                      y <- (getrand ymin ymax)
                                                      return $ Vector  x y
                area count size (x,y) = sequence (replicate count (randvectr (x,y) size))
                clusts = mapM (\clid -> do v <- randvect (0,1000) (0,1000)
                                           return $ Cluster {clId = clid,clCount=1,clSum=v}) [1..3]
             in do ear1 <- area 200 100 (200,200)
                   ear2 <- area 450 100 (800,200)
                   face <- area 1000 350 (500,600)
                   c <- clusts
                   writeFile "kmeans-points" (show (ear1++ear2++face))
                   writeFile "kmeans-clusters" (show c)
                

main = remoteInit "config" [Main.__remoteCallMetaData] initialProcess
