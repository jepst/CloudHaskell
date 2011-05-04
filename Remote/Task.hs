{-# LANGUAGE DeriveDataTypeable #-}
module Remote.Task where

import Remote.Call (putReg,getEntryByIdent)
import Remote.Encoding (hGetPayload,hPutPayload,Payload(..),getPayloadContent,Serializable,serialDecode,serialEncode,genericGet,genericPut)
import Remote.Process (getConfig,Config(..),matchProcessDown,terminate,nullPid,monitorProcess,TransmitException(..),MonitorAction(..),ptry,LogConfig(..),getLogConfig,setNodeLogConfig,setLogConfig,nodeFromPid,LogLevel(..),LogTarget(..),logS,getLookup,say,NodeId,ProcessM,ProcessId,PayloadDisposition(..),getSelfPid,getSelfNode,matchUnknownThrow,receiveWait,receiveTimeout,roundtripResponse,roundtripResponseAsync,roundtripQuery,match,invokeClosure,makePayloadClosure,spawn,spawnLocal,spawnLocalAnd,setDaemonic,send,makeClosure)
import Remote.Closure (Closure(..))
import Remote.Peer (getPeers)

import System.IO (withFile,IOMode(..))
import System.Directory (renameFile)
import Data.Data (Data)
import Data.Binary (Binary,get,put,putWord8,getWord8)
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

import Data.Digest.Pure.MD5 (md5)
import Data.ByteString.Lazy.UTF8 (fromString)
import qualified Data.ByteString.Lazy as B (concat)

import Debug.Trace

-- How does Erlang automatic restarts? OTP library etc

-- todo: implement disk storage

-- kmeans

-- mapreduce

-- autoshutdown worker nodes if their nodeboss goes down (via MaLink)

type ClosureWrapper = Dynamic

data PromiseStorage = PromiseInMemory PromiseData UTCTime
                    | PromiseOnDisk FilePath
                    | PromiseException String

type PromiseData = Payload
type TimeStamp = UTCTime

data MasterState = MasterState
     {
         msNodes :: MVar [(NodeId,ProcessId)],
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

data MmComplain = MmComplain ProcessId PromiseId deriving (Typeable,Data)
instance Binary MmComplain where get=genericGet; put=genericPut

data MmComplainResponse = MmComplainResponse ProcessId deriving (Typeable,Data)
instance Binary MmComplainResponse where get=genericGet; put=genericPut

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

makePromiseInMemory :: PromiseData -> IO PromiseStorage
makePromiseInMemory p = do utc <- liftIO $ getCurrentTime
                           return $ PromiseInMemory p utc

forwardLogs :: Maybe ProcessId -> ProcessM ()
forwardLogs masterpid = 
        do lc <- getLogConfig
           selfnid <- getSelfNode
           let newlc = lc {logTarget = case masterpid of
                                             Just mp
                                                | nodeFromPid mp /= selfnid -> LtForward $ nodeFromPid mp
                                             _ -> LtStdout}     
             in setLogConfig newlc >> setNodeLogConfig newlc

hashClosure :: Closure a -> String
hashClosure (Closure s pl) = show $ md5 $ B.concat [fromString s, getPayloadContent pl]

undiskify :: FilePath -> MVar PromiseStorage -> ProcessM (Maybe PromiseData)
undiskify fp mps =
      do wrap $ liftIO $ modifyMVar mps (\val ->
            case val of
              PromiseOnDisk fp -> 
                do pl <- withFile fp ReadMode hGetPayload
                   inmem <- makePromiseInMemory pl
                   return (inmem,Just pl)
              PromiseInMemory payload _ -> return (val,Just payload)
              _ -> return (val,Nothing))
  where wrap a = do res <- ptry a
                    case res of
                      Left e -> do logS "TSK" LoCritical $ "Error reading promise from file "++fp++": "++show (e::IOError)
                                   return Nothing
                      Right r -> return r

diskify :: FilePath -> MVar PromiseStorage -> ProcessM ()
diskify fp mps =
      do cfg <- getConfig
         receiveTimeout (cfgPromiseFlushDelay cfg) []
         wrap $ liftIO $ modifyMVar_ mps (\val ->
              case val of
                  PromiseInMemory payload _ ->
                      do liftIO $ withFile tmp WriteMode (\h -> hPutPayload h payload)
                         renameFile tmp fp
                         return $ PromiseOnDisk fp
                  _ -> return val)
   where tmp = fp ++ ".tmp"
         wrap a = do res <- ptry a
                     case res of
                         Left z -> do logS "TSK" LoImportant $ "Error writing promise to disk on file "++fp++": "++show (z::IOError)
                                      return ()
                         _ -> return ()

startNodeWorker :: ProcessId -> MVar (Map.Map PromiseId (MVar PromiseStorage)) -> MVar PromiseStorage -> Closure Payload -> ProcessM ()
startNodeWorker masterpid mpc mps clo@(Closure cloname cloarg) = 
  do self <- getSelfPid
     spawnLocalAnd (starter self) (return ())
     return ()
  where starter nodeboss = 
            let initialState = TaskState {tsMaster=masterpid,tsNodeBoss=Just nodeboss,tsPromiseCache=mpc,tsRedeemerForwarding=Map.empty}
                tasker = do tbl <- liftTask $ getLookup
                            case getEntryByIdent tbl cloname of
                              Just funval -> 
                                          do val <- funval cloarg
                                             p <- liftTaskIO $ makePromiseInMemory val
                                             liftTaskIO $ putMVar mps p
                                             liftTask $ diskify ("rpromise-"++hashClosure clo) mps
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
                    (\pc ->               let newpc = Map.insert promise promisestore pc
                                           in return (newpc,True))
                  when (ret)
                       (startNodeWorker masterpid promisecache promisestore clo)
                  return (NmStartResponse ret,promisecache)) 
            nmTermination = matchProcessDown masterpid $
                     do forwardLogs Nothing
                        logS "TSK" LoCritical $ "Orderly termination of nodeboss after my master is gone"
                        terminate
            nmRedeem = roundtripResponseAsync (\(NmRedeem promise) ans ->
                     let answerer = do pc <- liftIO $ readMVar promisecache
                                       case Map.lookup promise pc of
                                         Nothing -> ans (NmRedeemResponse Nothing)
                                         Just v -> do rv <- liftIO $ readMVar v -- possibly long wait
                                                      case rv of
                                                         PromiseInMemory rrv _ -> ans (NmRedeemResponse (Just rrv))
                                                         PromiseOnDisk fp -> do mpd <- undiskify fp v
                                                                                ans (NmRedeemResponse mpd)
                                                         PromiseException _ -> ans (NmRedeemResponse Nothing)
                      in do spawnLocal answerer
                            return promisecache)
         in receiveWait [nmStart, nmRedeem, nmTermination, matchUnknownThrow] >>= handler
   in do mypid <- getSelfPid
         monitorProcess mypid masterpid MaMonitor
         forwardLogs $ Just masterpid
         pc <- liftIO $ newMVar Map.empty
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
              let initialState = TaskState {tsMaster=master,tsNodeBoss=Just nodeboss,tsPromiseCache=pc,tsRedeemerForwarding=Map.empty}
              res <- liftM snd $ runTaskM proc initialState
              liftIO $ putMVar mvdone res

type LocationSelector = MasterState -> ProcessM (NodeId,ProcessId)

selectLocation :: MasterState -> ProcessM (NodeId,ProcessId)
selectLocation ms = 
   let nodes = msNodes ms
    in liftIO $ modifyMVar nodes
          (\n -> case n of
                    [] -> taskError "Attempt to allocate a task, but no nodes found"
                    _ -> return (rotate n,head n))
      where rotate [] = []
            rotate (h:t) = t ++ [h]

countLocations :: MasterState -> ProcessM Int
countLocations ms = liftIO $ withMVar (msNodes ms) (\a -> return $ length a)

allocateTask :: (NodeId,ProcessId) -> PromiseId -> Closure PromiseData -> ProcessM Bool
allocateTask (_,nodeboss) promise clo =
   do 
      res <- roundtripQuery PldUser nodeboss (NmStart promise clo)
      case res of
         Right (NmStartResponse True) -> return True
         _ -> return False

findPeers :: ProcessM [NodeId]
findPeers = liftM (concat . Map.elems) getPeers

sendSilent :: (Serializable a) => ProcessId -> a -> ProcessM ()
sendSilent pid a = do res <- ptry $ send pid a
                      case res of
                        Left (TransmitException _) -> return ()
                        Right _ -> return ()

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
           mapM_ (\nid -> sendSilent masterpid (TmNewPeer nid)) newlyadded
           return totalseen
     proberDelay = 60000000 -- one minute
     prober nodes seen masterpid =
        do totalseen <- probeOnce nodes seen masterpid
           receiveTimeout proberDelay [matchUnknownThrow]
           prober nodes totalseen masterpid
     master state = 
        let 
            tryAlloc clo promiseid =
              do loc@(nid,nodeboss) <- selectLocation state
                 res <- allocateTask loc promiseid clo
                 case res of
                   False -> 
                     do logS "TSK" LoImportant $ "Failed attempt to start "++show clo++" on " ++show nid
                        return Nothing
                   True -> return $ Just nodeboss
            basicAllocate clo promiseid =
               do count <- countLocations state
                  res <- stubborn count $ tryAlloc clo promiseid 
                  case res of
                      Nothing -> do logS "TSK" LoCritical $ "Terminally failed to start "++show clo
                                    return res
                      _ -> return res
            complainMsg = roundtripResponse
              (\x -> case x of
                       MmComplain procid promid -> 
                         case Map.lookup promid (msPromises state) of
                            Nothing -> return (MmComplainResponse nullPid,state) -- failure
                            Just (curprocid,curclo) 
                               | curprocid /= procid -> return (MmComplainResponse curprocid,state)
                               | otherwise -> 
                                   do res <- basicAllocate curclo promid
                                      case res of
                                        Nothing -> return (MmComplainResponse nullPid,state) -- failure
                                        Just newprocid ->
                                           let newpromises = Map.insert promid (newprocid,curclo) (msPromises state)
                                            in return (MmComplainResponse newprocid,state {msPromises=newpromises})) 
            promiseMsg = roundtripResponse 
              (\x -> case x of
                       MmNewPromise clo -> 
                          do 
                             promiseid <- liftIO $ newPromiseId -- TODO make this part of MasterState, not IO
                             res <- basicAllocate clo promiseid
                             case res of
                                Just nodeboss -> 
                                        let newstate = state {msAllocation=newAllocation,msPromises=newPromises}
                                            newAllocation = Map.insertWith' (\a b -> nub $ a++b) nodeboss [promiseid] (msAllocation state)
                                            newPromises = Map.insert promiseid (nodeboss,clo) (msPromises state)
                                         in return (MmNewPromiseResponse nodeboss promiseid,newstate)
                                Nothing -> 
                                         return (MmNewPromiseResponseFail,state))
            simpleMsg = match 
              (\x -> case x of
                       TmNewPeer nid -> return state) {- say ("Found new one " ++ show nid) >> return state) -}
         in receiveWait [simpleMsg, promiseMsg, complainMsg, matchUnknownThrow] >>= master
   in do nodes <- liftIO $ newMVar []
         selfnode <- getSelfNode
         selfpid <- getSelfPid
         let initState = MasterState {msAllocation=Map.empty, msPromises=Map.empty, msNodes=nodes}
         masterpid <- spawnDaemonic (master initState)
         seennodes <- probeOnce nodes [] masterpid
         res <- liftIO $ withMVar nodes (\n -> return $ lookup selfnode n)
         case res of
             Nothing -> taskError "Can't find self: make sure cfgKnownHosts includes the master"
             Just x -> spawnLocalAnd (masterproc x) (do myself <- getSelfPid
                                                        monitorProcess selfpid myself MaLinkError)
         spawnDaemonic (prober nodes seennodes masterpid)
         return masterpid

stubborn :: (Monad m) => Int -> m (Maybe a) -> m (Maybe a)
stubborn 0 a = a
stubborn n a | n>0
             = do r <- a
                  case r of
                     Just _ -> return r
                     Nothing -> stubborn (n-1) a

-- TODO: asPromise :: (Serializable a) => a -> TaskM (Promise a)
-- requires a new message, MmNewPromiseLiteral
-- (this might not be possible)

-- TODO: newPromiseWhere :: (Serializable a) => TaskLocality -> QueuingOption -> Closure (TaskM a) -> TaskM (Promise a)
-- where TaskLocality specifies how to select target node (default round-robin)
--       QueuingOption is a bool saying to start executing the task immediately, or to queue the task until all previously queued tasks are done

newPromise :: (Serializable a) => Closure (TaskM a) -> TaskM (Promise a)
newPromise clo = 
   let realclo = makePayloadClosure clo
    in case realclo of
          Just plclo -> do master <- getMaster
                           res <- liftTask $ roundtripQuery PldUser master (MmNewPromise plclo)
                           case res of
                              Right (MmNewPromiseResponse pid prid) -> return $ PromiseBasic pid prid
                              Right (MmNewPromiseResponseFail) ->
                                          taskError $ "Spawning of closure "++show clo++" by newPromise failed"
                              Left tms -> taskError $ "Spawning of closure "++show clo++" by newPromise resulted in "++show tms
          Nothing -> taskError $ "The specified closure, "++show clo++", can't produce payloads"

readPromise :: (Serializable a) => Promise a -> TaskM a
readPromise p = 
   let prhost = psRedeemer p
       prid = psId p
    in do mp <- lookupCachedPromise prid
          case mp of
            Nothing -> do fprhost <- lookupForwardedRedeemer prhost
                          res <- liftTask $ roundtripQuery PldUser fprhost (NmRedeem prid) -- possible long wait here
                          case res of
                             Left _ -> do master <- getMaster
                                          response <- liftTask $ roundtripQuery PldUser master (MmComplain fprhost prid)
                                          case response of
                                            Left a -> taskError $ "Couldn't file complaint with master about " ++ show fprhost ++ " because " ++ show a
                                            Right (MmComplainResponse newhost) 
                                              | newhost == nullPid -> taskError $ "Couldn't file complaint with master about " ++ show fprhost
                                              | otherwise -> do setForwardedRedeemer prhost newhost
                                                                readPromise p
                             Right (NmRedeemResponse pl) -> 
                                case pl of 
                                  Just thedata -> do pstore <- liftTaskIO $ makePromiseInMemory thedata
                                                     putPromiseInCache prid pstore
                                                     extractFromPayload thedata
                                  _ -> taskError "Failed promise redemption" -- don't redeem, this is a terminal failure, mostly likely the promise threw an exception
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
         tsPromiseCache :: MVar (Map.Map PromiseId (MVar PromiseStorage)),
         tsRedeemerForwarding :: Map.Map ProcessId ProcessId
      }

data TaskM a = TaskM { runTaskM :: TaskState -> ProcessM (TaskState, a) } deriving (Typeable)

instance Monad TaskM where
   m >>= k = TaskM $ \ts -> do
                (ts',a) <- runTaskM m ts
                (ts'',a') <- runTaskM (k a) (ts')
                return (ts'',a')              
   return x = TaskM $ \ts -> return $ (ts,x)

lookupForwardedRedeemer :: ProcessId -> TaskM ProcessId
lookupForwardedRedeemer q = 
   TaskM $ \ts ->
     case Map.lookup q (tsRedeemerForwarding ts) of
       Nothing -> return (ts,q)
       Just a -> return (ts,a)

setForwardedRedeemer :: ProcessId -> ProcessId -> TaskM ()
setForwardedRedeemer from to =
   TaskM $ \ts ->
     let newmap = Map.insert from to (tsRedeemerForwarding ts) 
      in return (ts {tsRedeemerForwarding=newmap},())

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
                   | PlNil deriving Typeable

instance (Binary a) => Binary (PromiseList a) where
   put (PlChunk a p) = putWord8 0 >> put a >> put p
   put PlNil = putWord8 1
   get = do w <- getWord8
            case w of
               0 -> do a <- get
                       p <- get
                       return $ PlChunk a p
               1 -> return PlNil

data Promise a = PromiseBasic { psRedeemer :: ProcessId, psId :: PromiseId } deriving Typeable
-- psRedeemer should maybe be wrapped in an IORef so that it can be updated in case of node failure

instance Binary (Promise a) where
   put (PromiseBasic a b) = put a >> put b
   get = do a <- get
            b <- get
            return $ PromiseBasic a b

remoteCallRectify :: (Serializable a) => ProcessM (TaskM a) -> TaskM Payload
remoteCallRectify x = 
         do a <- liftTask x
            res <- a
            liftTaskIO $ serialEncode res

