module Remote.Task where

data Promise a

data TaskM a

-- how to deal with datasets too big to fit in memory?

instance Monad TaskM

type RoleId = String

data TaskLocality = forall a.TlNear (Promise a)
                  | TlAt NodeId
                  | TlAutoByNumberOfTasks RoleId
                  | TlAutoByProcessorLoad RoleId

newPromise :: (Serializable a) => Closure (TaskM a) -> TaskM (Promise a)
newPromise = newPromiseWhere (TlAutoByNumberOfTasks "TASKWORKER")

newPromiseWhere :: (Serializable a) => TaskLocality -> Closure (TaskM a) -> TaskM (Promise a)

makePromise :: (Serializable a) => a -> TaskM (Promise a)

readPromise :: Promise a -> TaskM a

startWorkerTask :: ProcessM ()

startMasterTask :: TaskM a -> ProcessM a


data MapReduceTasks input inter key output 
                    = (Serializable input,
                       Serializable inter,
                       Serializable key,
                       Serializable output) => MapReduceTasks
     {
       mrtMapper :: Closure (Promise [input] -> TaskM [(inter,key)]),
       mrtReducer :: Closure (Promise [(inter,key)] -> TaskM [output]),
       mrtSlicer :: Closure ([input] -> TaskM [(NodeId,[input])]),
       mrtShuffler :: Closure ((inter,key) -> TaskM NodeId)
     }

defaultMapReduceTasks :: MapReduceTasks input inter key output
defaultMapReduceTasks = MapReduceTasks {mrtMapper = undefined,
                                        mrtReducer = undefined,
                                        mrtSlice = defaultMapReduceSlicer,
                                        mrtShuffler = defaultMapReduceShuffler}

mapReduce :: MapReduceTasks input inter key output -> [input] -> TaskM [output]

-- also, there should be a Stream data type that lets files be processed piecemeal to and from promises via files

data Stream a

--also, distributed map

mapRemote :: (Serializable a,Serializable b) => Closure (a -> b) -> [a] -> TaskM [b]


{-
TaskNotes
--------
Calling runTask starts a process (somewhere) which generates the required data and then goes into a receiveWait. The resulting Promise contains (a) the PID of that process (b) the PID of the master node of this network and (c) (probably not) the closure that started it. An attempt to redeem a promise will first check the local node's cache to see if the requested data has already been copied; if so, we're done. Otherwise, send a PromiseRedeem message to the given PID and expect a response containing the data; store the data in the local cache. If the PID does not exist, the node does not exist, or sends a negative response to PromiseRedeem, then we have to ask the master to restart the missing process. This is easy, because the closure to start it is contained in the Promise data structure (alternatively, the closure can be stored by the master). The master will also record that future accesses to the old (missing) PID should be redirected to the new one, so that other tasks attempting to redeem the same promise will be redirected after their first fail, e.g. the broken promise will be exchanged by the master for a new one. (Note that this assumes that two distinct processes will never have the same PID; this is not necessarily the case, so it might be a good idea to add a random unique identifier to node IDs.)
-}

--------------------

module Main where

import Remote.Task
import Remote.Init
import Remote.Process
import Remote.Call

import Control.Monad
import qualified Data.Map as Map

-- contains Vector, Cluster, ClusterId, clusterCenter, sqDistance
import KMeansCommon

$(remoteCall [d|
    kmeansMapper :: Promise [Vector] -> Promise [Cluster] -> TaskM [(Vector,ClusterId)]
    kmeansMapper ppoints pclusters = do points <- readPromise ppoints
                                        clusters <- readPromise pclusters
                                        return $ map (nearby clusters) points
                      where nearby clusters apoint = (apoint,clusterId $ minimumBy (sqDistance apoint) clusters)
    kmeansReducer :: Promise [(Vector,ClusterId)] -> TaskM [Cluster]
    kmeansReducer ppts = do pts <- readPromise ppts
                            let grps = groupBy ((\_,cid1) (_,cid2) -> cid1==cid2) pts
                            return $ map (\grp -> makeCluster (snd grp) $ map fst grp) grps
  |]

initialProcess :: RoleId -> ProcessM ()
initialProcess "TASKWORKER" = startWorkerTask
initialProcess "MASTER" = 
       do points <- liftIO $ liftM read $ readFile "kmeans-points" :: ProcessM [Vector]
          clusters <- liftIO $ liftM read $ readFile "kmeans-clusters" :: ProcessM [Cluster]
          result <- startMasterTask $ 
            do ppoints <- newPromise points
               let loop currentclusters = 
                   newclusters <- mapReduce (defaultMapReduceTask {mrtMapper=kmeansMapper__closure__partial points,mrtReducer=kmeansReducer__closure__partial}) currentclusters -- need some easy way to specify partial applications of closures
                   if newclusters==currentclusters
                      then return newclusters
                      else loop newclusters
               loop clusters
          say $ "Converged! " ++ show result

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess
