{-# LANGUAGE TemplateHaskell #-}
module Main where

import Remote

import Control.Monad.Trans
import Control.Monad
import Data.List (minimumBy)

import Debug.Trace

import KMeansCommon

type Line = String
type Word = String

mrMapper :: (Promise [Promise Vector], [Cluster]) -> TaskM [(ClusterId, Promise Vector)]
mrMapper (ppoints,clusters) =
   do points <- readPromise ppoints
      tsay $ "mapping "++show (length points)++" points and "++show (length clusters)++" clusters"
      mapM (assign (map (\c -> (clId c,clusterCenter c)) clusters)) points
  where assign clusters point =
           let distances point = map (\(clid,center) -> (clid,sqDistance center point)) clusters
               assignment point = fst $ minimumBy (\(_,a) (_,b) -> compare a b) (distances point)
            in do vp <- readPromise point
                  vp `seq` return (assignment vp,point)


mrReducer :: (ClusterId,[Promise Vector]) -> TaskM Cluster
mrReducer (cid,l) = 
   do tsay $ "reducing cluster id " ++ show cid ++ " with " ++ show (length l) ++" points"
      let emptyCluster = makeCluster cid []
       in foldM (\c pv -> do v <- readPromise pv
                             c `seq` return $ addToCluster c v) emptyCluster l

$( remotable ['mrMapper,  'mrReducer] )

again :: Int -> (b -> TaskM b) -> b -> TaskM b
again 0 f i = tsay "last iteration" >> f i
again n f i = do tsay (show n++" iterations remaining")
                 q <- f i
                 again (n-1) f q

initialProcess "MASTER" = 
                   do setNodeLogConfig defaultLogConfig {logLevel = LoInformation} 
                      clusters <- liftIO $ getClusters "kmeans-clusters"
                      points <- liftIO $ getPoints2 "kmeans-points" 
                      say $ "starting master"
                      ans <- runTask $
                        do
                           vpoints <- mapM toPromise points
                           ppoints <- mapM toPromise (chunkify 5 vpoints)
                           let myMapReduce = 
                                MapReduce 
                                {
                                 mtMapper = mrMapper__closure,
                                 mtReducer = mrReducer__closure,
                                 mtChunkify = \clusts -> [(ps,clusts) | ps <- ppoints],
                                 mtShuffle = shuffle
                                }
                           again 4 (mapReduce myMapReduce) clusters
                      say $ "done" -- show ans
initialProcess _ =  setNodeLogConfig defaultLogConfig {logLevel = LoInformation} 
                      >> say "starting worker" >> receiveWait [] 

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

