module Remote.Task where

import qualified Data.Map as Map (Map)
import Data.Dynamic (Dynamic)
import System.FilePath (FilePath)
import Data.Time (UTCTime)

type ClosureWrapper = Dynamic

data PromiseStorage = PromiseInMemory PromiseData TimeUTC
                    | PromiseOnDisk FilePath

type PromiseData = Payload
type TimeStamp = UTCTime

newNodeManager :: NodeId -> TaskM ProcessId
newWorker :: Closure a -> MVar WorkerState -> TaskM ProcessId


data MasterState = MasterState
     {
-- we need to
--   associate each promiseID with a nodeboss who can gives us its data
--   keep a list of nodebosses
--        for each nodeboss
--            which promises are available on that node
--            and associated closures for restarting
--            performance and locality information
--   all known promises
--       and which nodeboss owns them
-- node scheduling list, with ordering and load information
-- if a nodeboss goes down, remove it from lists
-- at regular intervals, rescan available workers
-- if a worker asks promise that can't be redeemed by a nodeboss (due to a restart),
--    exchange it for a new promise (same ID, new nodeboss)
     }


data TaskState = TaskState
      {
         tsMaster :: ProcessId,
         tsNodeBoss :: Maybe ProcessId,
         tsPromiseCache :: MVar (Map.Map PromiseId (MVar PromiseStorage))
      }

data TaskM a = { runTaskM :: TaskState -> ProcessM (TaskState, a) }

instance Monad TaskM where
   m >>= k = TaskM $ \ts -> do
                (ts',a) <- runTaskM m ts
                (ts'',a') <- runTaskM (k a) (ts')
                return (ts'',a')              
   return x = TaskM $ \ts -> return $ (ts,x)

liftTask :: ProcessM a -> TaskM a
liftTask a = TaskM $ \ts -> a >>= (\x -> return (ts,x))

liftTaskIO :: IO a -> TaskM a
liftTaskIO = liftTask . liftIO

newtype PromiseId = Int

data PromiseList a = PlChunk a (Promise (PromiseList a))
                   | PlNone
type ProiseList2 a = [Promise a]

data Promise a = PromiseBasic { psRedeemer :: ProcessId, psId :: PromiseId }

{-
type RoleId = String

data TaskLocality = forall a.TlNear (Promise a)
                  | TlAt NodeId
                  | TlAutoByNumberOfTasks RoleId
                  | TlAutoByProcessorLoad RoleId
-}

newPromise :: (Serializable a) => Closure (TaskM a) -> TaskM (Promise a)

-- newPromiseWhere :: (Serializable a) => TaskLocality -> Closure (TaskM a) -> TaskM (Promise a)

makePromise :: (Serializable a) => a -> TaskM (Promise a)

readPromise :: Promise a -> TaskM a
{- readpromise sends a message to the nodeboss listed in the Promise, waits for the data.
it also monitors the nodeboss; if it goes down, we contact the master and ask for a replacement,
then repeat -}

runWorkerTask :: ProcessM ()

runMasterTask :: TaskM a -> ProcessM a

{-
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

--also, distributed map

mapRemote :: (Serializable a,Serializable b) => Closure (a -> b) -> [a] -> TaskM [b]


-}
