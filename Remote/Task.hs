{-# LANGUAGE DeriveDataTypeable #-}
module Remote.Task where

import Remote.Call (putReg,getEntryByIdent)
import Remote.Encoding (Payload(..),Serializable,serialDecode,serialEncode,genericGet,genericPut)
import Remote.Process (monitorProcess,MonitorAction(..),ptry,LogConfig(..),getLogConfig,setLogConfig,nodeFromPid,LogLevel(..),LogTarget(..),logS,getLookup,say,NodeId,ProcessM,ProcessId,PayloadDisposition(..),getSelfPid,getSelfNode,matchUnknownThrow,receiveWait,receiveTimeout,roundtripResponse,roundtripResponseAsync,roundtripQuery,match,invokeClosure,makePayloadClosure,spawn,spawnLocal,spawnLocalAnd,setDaemonic,send,makeClosure)
import Remote.Closure (Closure(..))
import Remote.Peer (getPeers)

import Data.Data (Data)
import Data.Binary (Binary,get,put)
import Control.Exception (SomeException,Exception,throw)
import Data.Unique (hashUnique,newUnique)
import Data.Typeable (Typeable)
import Control.Monad (liftM,when)
import Control.Monad.Trans (liftIO)
import Control.Concurrent.MVar (MVar,modifyMVar,modifyMVar_,newMVar,newEmptyMVar,takeMVar,putMVar,readMVar,withMVar)
import qualified Data.Map as Map (Map,insert,lookup,empty,elems,insertWith')
import Data.List ((\\),union,nub)
import Data.Dynamic (Dynamic)
import System.FilePath (FilePath)
import Data.Time (UTCTime,getCurrentTime)

import Debug.Trace

-- How does Erlang automatic restarts? OTP library etc

-- the filename used to store saved promises should be based on a hash of the corresponding function name and it arguments

-- todo: implement disk storage, fault tolerance, mapreduce, kmeans example

type ClosureWrapper = Dynamic

data PromiseStorage = PromiseInMemory PromiseData UTCTime
                    | PromiseOnDisk FilePath
                    | PromiseException String

type PromiseData = Payload
type TimeStamp = UTCTime

data MasterState = MasterState
     {
         msNodes :: MVar [(NodeId,ProcessId)], -- will be updated asyncronously by getPeers thread
         msAllocation :: Map.Map ProcessId [PromiseId],
         msPromises :: Map.Map PromiseId (ProcessId,Closure PromiseData)
     }

spawnDaemonic :: ProcessM () -> ProcessM ProcessId
spawnDaemonic p = spawnLocalAnd p setDaemonic

runWorkerNode :: ProcessId -> NodeId -> ProcessM ProcessId
runWorkerNode masterpid nid = 
     do clo <- makeClosure "runWorkerNode__impl" (masterpid) :: ProcessM (Closure (ProcessM ()))
        spawn nid clo

runWorkerNode__impl :: Payload -> ProcessM ()
runWorkerNode__impl pl = 
   do setDaemonic
      mpid <- liftIO $ serialDecode pl
      case mpid of
         Just masterpid -> handler masterpid
         Nothing -> error "Failure to extract in rwn__impl"
 where handler masterpid = startNodeManager masterpid

__remoteCallMetaData x = putReg runWorkerNode__impl "runWorkerNode__impl" x

data MmNewPromise = MmNewPromise (Closure Payload) deriving (Typeable)
instance Binary MmNewPromise where 
  get = do a <- get
           return $ MmNewPromise a
  put (MmNewPromise a) = put a

data MmNewPromiseResponse = MmNewPromiseResponse ProcessId PromiseId 
                          | MmNewPromiseResponseFail deriving (Typeable,Data)
instance Binary MmNewPromiseResponse where get=genericGet; put=genericPut

data TmNewPeer = TmNewPeer NodeId deriving (Typeable,Data)
instance Binary TmNewPeer where get=genericGet; put=genericPut

data NmStart = NmStart PromiseId (Closure Payload) deriving (Typeable)
instance Binary NmStart where 
  get = do a <- get
           b <- get
           return $ NmStart a b
  put (NmStart a b) = put a >> put b

data NmStartResponse = NmStartResponse Bool deriving (Typeable,Data)
instance Binary NmStartResponse where get=genericGet; put=genericPut
data NmRedeem = NmRedeem PromiseId deriving (Typeable,Data)
instance Binary NmRedeem where get=genericGet; put=genericPut
data NmRedeemResponse = NmRedeemResponse (Maybe Payload) deriving (Typeable)
instance Binary NmRedeemResponse where 
   get = do a <- get
            return $ NmRedeemResponse a 
   put (NmRedeemResponse a) = put a

data TaskException = TaskException String deriving (Show,Typeable)
instance Exception TaskException

taskError :: String -> a
taskError s = throw $ TaskException s

newPromiseId :: IO PromiseId
newPromiseId = do d <- newUnique
                  return $ hashUnique d

makePromiseInMemory :: PromiseData -> ProcessM PromiseStorage
makePromiseInMemory p = do utc <- liftIO $ getCurrentTime
                           return $ PromiseInMemory p utc

forwardLogs :: ProcessId -> ProcessM ()
forwardLogs masterpid = 
    let masternid = (nodeFromPid masterpid)
     in do lc <- getLogConfig
           selfnid <- getSelfNode
           when (selfnid /= masternid)       
              (setLogConfig lc {logTarget = LtForward masternid})

startNodeWorker :: ProcessId -> MVar (Map.Map PromiseId (MVar PromiseStorage)) -> MVar PromiseStorage -> Closure Payload -> ProcessM ()
startNodeWorker masterpid mpc mps (Closure cloname cloarg) = 
  do self <- getSelfPid
     spawnLocalAnd (starter self) (forwardLogs masterpid)
     return ()
  where starter nodeboss = 
            let initialState = TaskState {tsMaster=masterpid,tsNodeBoss=Just nodeboss,tsPromiseCache=mpc}
                tasker = do tbl <- liftTask $ getLookup
                            case getEntryByIdent tbl cloname of
                              Just funval -> 
                                          do val <- funval cloarg
                                             p <- liftTask $ makePromiseInMemory val
                                             liftTaskIO $ putMVar mps p
                              Nothing -> taskError $ "Failed looking up "++cloname++" in closure table"
             in do res <- ptry $ runTaskM tasker initialState :: ProcessM (Either SomeException (TaskState,()))
                   case res of
                     Left ex -> liftIO (putMVar mps (PromiseException (show ex))) >> throw ex
                     Right _ -> return ()

startNodeManager :: ProcessId -> ProcessM ()
startNodeManager masterpid = 
  let
      handler :: MVar (Map.Map PromiseId (MVar PromiseStorage)) -> ProcessM a
      handler promisecache = 
        let nmStart = roundtripResponse (\(NmStart promise clo) -> 
               do promisestore <- liftIO $ newEmptyMVar
                  ret <- liftIO $ modifyMVar promisecache 
                    (\pc -> case Map.lookup promise pc of
                               Just _ -> return (pc,False)
                               Nothing -> let newpc = Map.insert promise promisestore pc
                                           in return (newpc,True))
                  when (ret)
                       (startNodeWorker masterpid promisecache promisestore clo)
                  return (NmStartResponse ret,promisecache)) 
            nmRedeem = roundtripResponseAsync (\(NmRedeem promise) ans ->
                     let answerer = do pc <- liftIO $ readMVar promisecache
                                       case Map.lookup promise pc of
                                         Nothing -> ans (NmRedeemResponse Nothing)
                                         Just v -> do rv <- liftIO $ readMVar v -- possibly long wait
                                                      case rv of
                                                         PromiseInMemory rrv _ -> ans (NmRedeemResponse (Just rrv))
                                                         PromiseOnDisk _ -> error "Still can't do disk promises"
                                                         PromiseException _ -> ans (NmRedeemResponse Nothing)
                      in do spawnLocal answerer
                            return promisecache)
         in receiveWait [nmStart, nmRedeem, matchUnknownThrow] >>= handler
   in do pc <- liftIO $ newMVar Map.empty
         handler pc

startMaster :: TaskM a -> ProcessM a
startMaster proc = 
    do mvmaster <- liftIO $ newEmptyMVar
       mvdone <- liftIO $ newEmptyMVar
       master <- runMaster (masterproc mvdone mvmaster)
       liftIO $ putMVar mvmaster master
       liftIO $ takeMVar mvdone
   where masterproc mvdone mvmaster nodeboss = 
           do master <- liftIO $ takeMVar mvmaster
              pc <- liftIO $ newMVar Map.empty
              let initialState = TaskState {tsMaster=master,tsNodeBoss=Just nodeboss,tsPromiseCache=pc}
              res <- liftM snd $ runTaskM proc initialState
              liftIO $ putMVar mvdone res

selectLocation :: MasterState -> ProcessM (NodeId,ProcessId)
selectLocation ms = 
   let nodes = msNodes ms
    in liftIO $ modifyMVar nodes
          (\n -> case n of
                    [] -> taskError "Attempt to allocate a task, but no nodes found"
                    _ -> return (rotate n,head n))
      where rotate [] = []
            rotate (h:t) = t ++ [h]

allocateTask :: (NodeId,ProcessId) -> PromiseId -> Closure PromiseData -> ProcessM Bool
allocateTask (_,nodeboss) promise clo =
   do 
      res <- roundtripQuery PldUser nodeboss (NmStart promise clo)
      case res of
         Right (NmStartResponse True) -> return True
         _ -> return False

findPeers :: ProcessM [NodeId]
findPeers = liftM (concat . Map.elems) getPeers

runMaster :: (ProcessId -> ProcessM ()) -> ProcessM ProcessId
runMaster masterproc = 
  let
     probeOnce nodes seen masterpid =
        do recentlist <- findPeers
           let newseen = seen `union` recentlist
           let topidlist = recentlist \\ seen
           newlypidded <- mapM (\nid -> 
                             do pid <- runWorkerNode masterpid nid
                                return (nid,pid)) topidlist
           
           (newlist,totalseen) <- liftIO $ modifyMVar nodes (\oldlist -> 
                        return (oldlist ++ newlypidded,(recentlist,newseen)))
           let newlyadded = totalseen \\ seen
           mapM_ (\nid -> send masterpid (TmNewPeer nid)) newlyadded
           return totalseen
     proberDelay = 60000000 -- one minute
     prober nodes seen masterpid =
        do totalseen <- probeOnce nodes seen masterpid
           receiveTimeout proberDelay [matchUnknownThrow]
           prober nodes totalseen masterpid
     master state = 
        let 
            promiseMsg = roundtripResponse 
              (\x -> case x of
                       MmNewPromise clo -> 
                          do loc@(nid,nodeboss) <- selectLocation state
                             promiseid <- liftIO $ newPromiseId
                             res <- allocateTask loc promiseid clo
                             case res of
                                True -> let newstate = state {msAllocation=newAllocation,msPromises=newPromises}
                                            newAllocation = Map.insertWith' (\a b -> nub $ a++b) nodeboss [promiseid] (msAllocation state)
                                            newPromises = Map.insert promiseid (nodeboss,clo) (msPromises state)
                                         in return (MmNewPromiseResponse nodeboss promiseid,newstate)
                                False -> do logS "TSK" LoCritical $ "Failed attempt to start "++show clo++" on "++show nid
                                            return (MmNewPromiseResponseFail,state))
            simpleMsg = match 
              (\x -> case x of
                       TmNewPeer nid -> say ("Found new one " ++ show nid) >> return state)
         in receiveWait [simpleMsg, promiseMsg, matchUnknownThrow] >>= master
   in do nodes <- liftIO $ newMVar []
         selfnode <- getSelfNode
         selfpid <- getSelfPid
         let initState = MasterState {msAllocation=Map.empty, msPromises=Map.empty, msNodes=nodes}
         masterpid <- spawnDaemonic (master initState)
         probeOnce nodes [] masterpid
         res <- liftIO $ withMVar nodes (\n -> return $ lookup selfnode n)
         case res of
             Nothing -> taskError "Can't find self"
             Just x -> spawnLocalAnd (masterproc x) (do myself <- getSelfPid
                                                        monitorProcess selfpid myself MaLinkError)
         spawnDaemonic (prober nodes [] masterpid)
         return masterpid

newPromise :: (Serializable a) => Closure (TaskM a) -> TaskM (Promise a)
newPromise clo = 
   let realclo = makePayloadClosure clo
    in case realclo of
          Just plclo -> do master <- getMaster
                           res <- liftTask $ roundtripQuery PldUser master (MmNewPromise plclo)
                           case res of
                              Right (MmNewPromiseResponse pid prid) -> return $ PromiseBasic pid prid
                              Right (MmNewPromiseResponseFail) -> -- TODO retry
                                          taskError $ "Spawning of closure "++show clo++" by newPromise failed"
                              Left tms -> taskError $ "Spawning of closure "++show clo++" by newPromise resulted in "++show tms
          Nothing -> taskError $ "The specified closure, "++show clo++", can't produce payloads"

readPromise :: (Serializable a) => Promise a -> TaskM a
readPromise p = 
   let prhost = psRedeemer p
       prid = psId p
    in do mp <- lookupCachedPromise prid
          case mp of
            Nothing -> do res <- liftTask $ roundtripQuery PldUser prhost (NmRedeem prid) -- possible long wait here
                          case res of
                             Left _ -> error "I should re-redeem via the master at this point"
                             Right (NmRedeemResponse pl) -> 
                                case pl of 
                                  Just thedata -> do pstore <- liftTask $ makePromiseInMemory thedata
                                                     putPromiseInCache prid pstore
                                                     extractFromPayload thedata
                                  _ -> error "Failed promise redemption" -- don't redeem, this is a terminal failure
            Just mv -> do val <- liftTaskIO $ readMVar mv -- possible long wait here
                          case val of
                             PromiseInMemory v _ -> extractFromPayload v
                             PromiseException _ -> taskError $ "Redemption of promise failed"
                             PromiseOnDisk _ -> error "PromiseOnDisk not yet supported"
     where extractFromPayload v = do out <- liftTaskIO $ serialDecode v
                                     case out of
                                       Just r -> return r
                                       Nothing -> taskError "Unexpected payload type"


data TaskState = TaskState
      {
         tsMaster :: ProcessId,
         tsNodeBoss :: Maybe ProcessId,
         tsPromiseCache :: MVar (Map.Map PromiseId (MVar PromiseStorage))
      }

data TaskM a = TaskM { runTaskM :: TaskState -> ProcessM (TaskState, a) } deriving (Typeable)

instance Monad TaskM where
   m >>= k = TaskM $ \ts -> do
                (ts',a) <- runTaskM m ts
                (ts'',a') <- runTaskM (k a) (ts')
                return (ts'',a')              
   return x = TaskM $ \ts -> return $ (ts,x)

lookupCachedPromise :: PromiseId -> TaskM (Maybe (MVar PromiseStorage))
lookupCachedPromise prid = TaskM $ \ts -> 
               do mv <- liftIO $ withMVar (tsPromiseCache ts)
                      (\pc -> return $ Map.lookup prid pc)
                  return (ts,mv)

putPromiseInCache :: PromiseId -> PromiseStorage -> TaskM ()
putPromiseInCache prid ps = TaskM $ \ts ->
        do liftIO $ modifyMVar_ (tsPromiseCache ts)
                (\pc -> do mv <- newMVar ps
                           return $ Map.insert prid mv pc)
           return (ts,())

getMaster :: TaskM ProcessId
getMaster = TaskM $ \ts -> return (ts,tsMaster ts)

liftTask :: ProcessM a -> TaskM a
liftTask a = TaskM $ \ts -> a >>= (\x -> return (ts,x))

liftTaskIO :: IO a -> TaskM a
liftTaskIO = liftTask . liftIO

type PromiseId = Int

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

-- newPromiseWhere :: (Serializable a) => TaskLocality -> Closure (TaskM a) -> TaskM (Promise a)

-- makePromise :: (Serializable a) => a -> TaskM (Promise a)


remoteCallRectify :: (Serializable a) => ProcessM (TaskM a) -> TaskM Payload
remoteCallRectify x = 
         do a <- liftTask x
            res <- a
            liftTaskIO $ serialEncode res

