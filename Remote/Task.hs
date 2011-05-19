{-# LANGUAGE DeriveDataTypeable #-}

-- | This module provides data dependency resolution and
-- fault tolerance via /promises/ (known elsewhere as /futures/).
-- It's implemented in terms of the "Remote.Process" module.
module Remote.Task (
                   -- * Tasks and promises
                   TaskM, Promise, PromiseList(..),
                   runTask,
                   newPromise, newPromiseAt, newPromiseNear, newPromiseHere, newPromiseAtRole,
                   toPromise, toPromiseAt, toPromiseNear, toPromiseImm,
                   readPromise, 

                   -- * MapReduce
                   MapReduce(..),
                   mapReduce,

                   -- * Useful auxilliaries
                   chunkify,
                   shuffle,
                   tsay,
                   tlogS,
                   Locality(..),
                   TaskException(..),

                   -- * Internals, not for general use
                   __remoteCallMetaData,
                   remoteCallRectify,
                   ) where

import Remote.Reg (putReg,getEntryByIdent)
import Remote.Encoding (serialEncodePure,hGetPayload,hPutPayload,Payload(..),getPayloadContent,Serializable,serialDecode,serialEncode)
import Remote.Process (roundtripQuery, roundtripQueryUnsafe, ServiceException(..), spawnAnd, AmSpawnOptions(..), TransmitStatus(..),diffTime,getConfig,Config(..),matchProcessDown,terminate,nullPid,monitorProcess,TransmitException(..),MonitorAction(..),ptry,LogConfig(..),getLogConfig,setNodeLogConfig,setLogConfig,nodeFromPid,LogLevel(..),LogTarget(..),logS,getLookup,say,LogSphere,NodeId,ProcessM,ProcessId,PayloadDisposition(..),getSelfPid,getSelfNode,matchUnknownThrow,receiveWait,receiveTimeout,roundtripResponse,roundtripResponseAsync,roundtripQueryImpl,match,invokeClosure,makePayloadClosure,spawn,spawnLocal,spawnLocalAnd,setDaemonic,send,makeClosure)
import Remote.Closure (Closure(..))
import Remote.Peer (getPeers)

import Data.Dynamic (Dynamic, toDyn, fromDynamic)
import System.IO (withFile,IOMode(..))
import System.Directory (renameFile)
import Data.Binary (Binary,get,put,putWord8,getWord8)
import Control.Exception (SomeException,Exception,throw,try)
import Data.Typeable (Typeable)
import Control.Monad (liftM,when)
import Control.Monad.Trans (liftIO)
import Control.Concurrent.MVar (MVar,modifyMVar,modifyMVar_,newMVar,newEmptyMVar,takeMVar,putMVar,readMVar,withMVar)
import qualified Data.Map as Map (Map,fromList,insert,lookup,empty,elems,insertWith',toList)
import Data.List ((\\),union,nub,groupBy,sortBy,delete,intercalate)
import System.FilePath (FilePath)
import Data.Time (UTCTime,getCurrentTime)

-- imports required for hashClosure; is there a lighter-weight of doing this?
import Data.Digest.Pure.MD5 (md5)
import Data.ByteString.Lazy.UTF8 (fromString)
import qualified Data.ByteString.Lazy as B (concat)

----------------------------------------------
-- * Promises and tasks
----------------------------------------------


type PromiseId = Integer

type Hash = String

data PromiseList a = PlChunk a (Promise (PromiseList a))
                   | PlNil deriving Typeable

instance (Serializable a) => Binary (PromiseList a) where
   put (PlChunk a p) = putWord8 0 >> put a >> put p
   put PlNil = putWord8 1
   get = do w <- getWord8
            case w of
               0 -> do a <- get
                       p <- get
                       return $ PlChunk a p
               1 -> return PlNil

-- | The basic data type for expressing data dependence
-- in the 'TaskM' monad. A Promise represents a value that
-- may or may not have been computed yet; thus, it's like
-- a distributed thunk (in the sense of a non-strict unit
-- of evaluation). These are created by 'newPromise' and friends,
-- and the underlying value can be gotten with 'readPromise'.
data Promise a = PromiseBasic { psRedeemer :: ProcessId, psId :: PromiseId } 
               | PromiseImmediate a deriving Typeable
-- psRedeemer should maybe be wrapped in an IORef so that it can be updated in case of node failure

instance (Serializable a) => Binary (Promise a) where
   put (PromiseBasic a b) = putWord8 0 >> put a >> put b
   put (PromiseImmediate a) = putWord8 1 >> put a
   get = do a <- getWord8
            case a of
              0 -> do b <- get
                      c <- get
                      return $ PromiseBasic b c
              1 -> do b <- get
                      return $ PromiseImmediate b

-- | Stores the data produced by a promise, in one of its
-- various forms. If it's currently in memory, we keep it
-- as a payload, to be decoded by its ultimate user (who
-- of course has the static type information), the time
-- it was last touched (so we know when to flush it),
-- and perhaps also a decoded version, so that it doesn't
-- need to be decoded repeatedly: this makes this go a lot
-- faster. If it's been flushed to disk, we keep track of
-- where, and if the promise didn't complete, but threw
-- an exception during its execution, we mark there here
-- as well: the exception will be propagated to
-- dependents.
data PromiseStorage = PromiseInMemory PromiseData UTCTime (Maybe Dynamic)
                    | PromiseOnDisk FilePath
                    | PromiseException String

type PromiseData = Payload
type TimeStamp = UTCTime

-- | Keeps track of what we know about currently running promises.
-- The closure and locality and provided by the initial call to
-- newPromise, the nodeboss is where it is currently running.
-- We need this info to deal with complaints.
data PromiseRecord = PromiseRecord ProcessId (Closure PromiseData) Locality

data MasterState = MasterState
     {
         -- | Promise IDs are allocated serially from here
         msNextId :: PromiseId,
 
         -- | All currently known nodes, with the role, node ID, and node boss. Updated asychronously by prober thread
         msNodes :: MVar [(String,NodeId,ProcessId)],

         -- | Given a nodeboss, which promises belong to it. Not sure what this is good for
         msAllocation :: Map.Map ProcessId [PromiseId],

         -- | Given a promise, what do we know about it. Include its nodeboss, its closure, and its locality preference
         msPromises :: Map.Map PromiseId PromiseRecord,

         -- | The locality preference of new worker tasks, if not specified otherwise
         msDefaultLocality :: Locality
     }

data MmNewPromise = MmNewPromise (Closure Payload) Locality Queueing deriving (Typeable)
instance Binary MmNewPromise where 
  get = do a <- get
           l <- get
           q <- get
           return $ MmNewPromise a l q
  put (MmNewPromise a l q) = put a >> put l >> put q

data MmNewPromiseResponse = MmNewPromiseResponse ProcessId PromiseId 
                          | MmNewPromiseResponseFail deriving (Typeable)
instance Binary MmNewPromiseResponse where 
   put (MmNewPromiseResponse a b) =
           do putWord8 0
              put a
              put b
   put MmNewPromiseResponseFail = putWord8 1
   get = do a <- getWord8
            case a of
              0 -> do b <- get 
                      c <- get
                      return $ MmNewPromiseResponse b c
              1 -> return MmNewPromiseResponseFail

data MmStatus = MmStatus deriving Typeable
instance Binary MmStatus where
  get = return MmStatus
  put MmStatus = put ()

data MmStatusResponse = MmStatusResponse [NodeId] (Map.Map ProcessId [PromiseId]) deriving Typeable
instance Binary MmStatusResponse where
  get = do a <- get
           b <- get
           return $ MmStatusResponse a b
  put (MmStatusResponse a b) = put a >> put b

data MmComplain = MmComplain ProcessId PromiseId deriving (Typeable)
instance Binary MmComplain where 
   put (MmComplain a b) = put a >> put b
   get = do a <- get
            b <- get
            return $ MmComplain a b

data MmComplainResponse = MmComplainResponse ProcessId deriving (Typeable)
instance Binary MmComplainResponse where
   put (MmComplainResponse a) = put a
   get = do a <- get
            return $ MmComplainResponse a

data TmNewPeer = TmNewPeer NodeId deriving (Typeable)
instance Binary TmNewPeer where 
   get = do a <- get
            return $ TmNewPeer a
   put (TmNewPeer nid) = put nid

data NmStart = NmStart PromiseId (Closure Payload) Queueing deriving (Typeable)
instance Binary NmStart where 
  get = do a <- get
           b <- get
           c <- get
           return $ NmStart a b c
  put (NmStart a b c) = put a >> put b >> put c

data NmStartResponse = NmStartResponse Bool deriving (Typeable)
instance Binary NmStartResponse where 
  get = do a <- get
           return $ NmStartResponse a
  put (NmStartResponse a) = put a

data NmRedeem = NmRedeem PromiseId deriving (Typeable)
instance Binary NmRedeem where 
   get = do a <- get
            return $ NmRedeem a
   put (NmRedeem prid) = put prid

data NmRedeemResponse = NmRedeemResponse Payload
                      | NmRedeemResponseUnknown
                      | NmRedeemResponseException deriving (Typeable)
instance Binary NmRedeemResponse where 
   get = do a <- getWord8
            case a of
              0 -> do b <- get
                      return $ NmRedeemResponse b
              1 -> return NmRedeemResponseUnknown
              2 -> return NmRedeemResponseException
   put (NmRedeemResponse a) = putWord8 0 >> put a
   put (NmRedeemResponseUnknown) = putWord8 1
   put (NmRedeemResponseException) = putWord8 2

data TaskException = TaskException String deriving (Show,Typeable)
instance Exception TaskException

-- | (Currently ignored.)
data Queueing = QuNone
              | QuExclusive
              | QuSmall deriving (Typeable,Ord,Eq)

defaultQueueing :: Queueing
defaultQueueing = QuNone

instance Binary Queueing where
   put QuNone = putWord8 0
   put QuExclusive = putWord8 1
   put QuSmall = putWord8 2
   get = do a <- getWord8
            case a of
               0 -> return QuNone
               1 -> return QuExclusive
               2 -> return QuSmall

-- | A specification of preference
-- of where a promise should be allocated,
-- among the nodes visible to the master.
data Locality = LcUnrestricted -- ^ The promise can be placed anywhere.
              | LcDefault -- ^ The default preference is applied, which is for nodes having a role of NODE of WORKER
              | LcByRole [String] -- ^ Nodes having the given roles will be preferred
              | LcByNode [NodeId] -- ^ The given nodes will be preferred

instance Binary Locality where
   put LcUnrestricted = putWord8 0
   put LcDefault = putWord8 1
   put (LcByRole a) = putWord8 2 >> put a
   put (LcByNode a) = putWord8 3 >> put a
   get = do a <- getWord8
            case a of
               0 -> return LcUnrestricted
               1 -> return LcDefault
               2 -> do r <- get
                       return $ LcByRole r
               3 -> do r <- get
                       return $ LcByNode r

defaultLocality :: Locality
defaultLocality = LcByRole ["WORKER","NODE"]

taskError :: String -> a
taskError s = throw $ TaskException s

monitorTask :: ProcessId -> ProcessId -> ProcessM TransmitStatus
monitorTask monitor monitee
   = do res <- ptry $ monitorProcess monitor monitee MaMonitor 
        case res of
           Right _ -> return QteOK
           Left (ServiceException e) -> return $ QteOther e

roundtripImpl :: (Serializable a, Serializable b) => ProcessId -> a -> ProcessM (Either TransmitStatus b)
roundtripImpl pid dat = roundtripQueryImpl 0 PldUser pid dat id []

roundtrip :: (Serializable a, Serializable b) => ProcessId -> a -> TaskM (Either TransmitStatus b)
roundtrip apid dat = 
   TaskM $ \ts ->
     case Map.lookup apid (tsMonitoring ts) of
       Nothing -> do mypid <- getSelfPid
                     res0 <- monitorTask mypid apid
                     case res0 of
                        QteOK -> 
                          do res <- roundtripImpl apid dat
                             return (ts {tsMonitoring=Map.insert apid () (tsMonitoring ts)},res)
                        _ -> return (ts,Left res0)
       Just _ -> do res <- roundtripImpl apid dat
                    return (ts,res)

-- roundtrip a b = liftTask $ roundtripQueryUnsafe PldUser a b

spawnDaemonic :: ProcessM () -> ProcessM ProcessId
spawnDaemonic p = spawnLocalAnd p setDaemonic

runWorkerNode :: ProcessId -> NodeId -> ProcessM ProcessId
runWorkerNode masterpid nid = 
     do clo <- makeClosure "Remote.Task.runWorkerNode__impl" (masterpid) :: ProcessM (Closure (ProcessM ()))
        spawn nid clo 

runWorkerNode__impl :: Payload -> ProcessM ()
runWorkerNode__impl pl = 
   do setDaemonic -- maybe it's good to have the node manager be daemonic, but prolly not. If so, the MASTERPID must be terminated when user-provided MASTERPROC ends
      mpid <- liftIO $ serialDecode pl
      case mpid of
         Just masterpid -> handler masterpid
         Nothing -> error "Failure to extract in rwn__impl"
 where handler masterpid = startNodeManager masterpid

passthrough__implPl :: Payload -> TaskM Payload
passthrough__implPl pl = return pl

passthrough__closure :: (Serializable a) => a -> Closure (TaskM a)
passthrough__closure a = Closure "Remote.Task.passthrough__impl" (serialEncodePure a)

__remoteCallMetaData x = putReg runWorkerNode__impl "Remote.Task.runWorkerNode__impl" 
                        (putReg passthrough__implPl "Remote.Task.passthrough__implPl" x)

updatePromiseInMemory :: PromiseStorage -> IO PromiseStorage
updatePromiseInMemory (PromiseInMemory p _ d) = do utc <- getCurrentTime
                                                   return $ PromiseInMemory p utc d
updatePromiseInMemory other = return other

makePromiseInMemory :: PromiseData -> Maybe Dynamic -> IO PromiseStorage
makePromiseInMemory p dyn = do utc <- getCurrentTime
                               return $ PromiseInMemory p utc dyn

forwardLogs :: Maybe ProcessId -> ProcessM ()
forwardLogs masterpid = 
        do lc <- getLogConfig
           selfnid <- getSelfNode
           let newlc = lc {logTarget = case masterpid of
                                             Just mp
                                                | nodeFromPid mp /= selfnid -> LtForward $ nodeFromPid mp
                                             _ -> LtStdout}     
             in setNodeLogConfig newlc

hashClosure :: Closure a -> Hash
hashClosure (Closure s pl) = show $ md5 $ B.concat [fromString s, getPayloadContent pl]

undiskify :: FilePath -> MVar PromiseStorage -> ProcessM (Maybe PromiseData)
undiskify fp mps =
      do wrap $ liftIO $ modifyMVar mps (\val ->
            case val of
              PromiseOnDisk fp -> 
                do pl <- withFile fp ReadMode hGetPayload
                   inmem <- makePromiseInMemory pl Nothing
                   return (inmem,Just pl)
              PromiseInMemory payload _ _ -> return (val,Just payload)
              _ -> return (val,Nothing))
  where wrap a = do res <- ptry a
                    case res of
                      Left e -> do logS "TSK" LoCritical $ "Error reading promise from file "++fp++": "++show (e::IOError)
                                   return Nothing
                      Right r -> return r

diskify :: FilePath -> MVar PromiseStorage -> Bool -> ProcessM ()
diskify fp mps reallywrite =
      do cfg <- getConfig
         when (cfgPromiseFlushDelay cfg > 0)
            (handler (cfgPromiseFlushDelay cfg))
    where
         handler delay =
           do receiveTimeout delay []
              again <- wrap $ liftIO $ modifyMVar mps (\val ->
                   case val of
                       PromiseInMemory payload utc _ ->
                           do now <- getCurrentTime
                              if diffTime now utc > delay
                                 then do when reallywrite $
                                             do liftIO $ withFile tmp WriteMode (\h -> hPutPayload h payload)
                                                renameFile tmp fp
                                         return (PromiseOnDisk fp,False)
                                 else return (val,True)
                       _      -> return (val,False))
              when again
                 (diskify fp mps reallywrite)
         tmp = fp ++ ".tmp"
         wrap a = do res <- ptry a
                     case res of
                         Left z -> do logS "TSK" LoImportant $ "Error writing promise to disk on file "++fp++": "++show (z::IOError)
                                      return False
                         Right v -> return v

startNodeWorker :: ProcessId -> NodeBossState -> 
                   MVar PromiseStorage -> Closure Payload -> ProcessM ()
startNodeWorker masterpid nbs mps clo@(Closure cloname cloarg) = 
  do self <- getSelfPid
     spawnLocalAnd (starter self) (prefix self)
     return ()
  where 
        prefix nodeboss =
          do self <- getSelfPid
             monitorProcess self nodeboss MaLink
             setDaemonic 
        starter nodeboss = -- TODO try to do an undiskify here, if the promise is left over from a previous, failed run
            let initialState = TaskState {tsMaster=masterpid,tsNodeBoss=Just nodeboss, 
                                          tsPromiseCache=nsPromiseCache nbs, tsRedeemerForwarding=nsRedeemerForwarding nbs,
                                          tsMonitoring=Map.empty}
                tasker = do tbl <- liftTask $ getLookup
                            case getEntryByIdent tbl cloname of
                              Just funval -> 
                                          do val <- funval cloarg
                                             p <- liftTaskIO $ makePromiseInMemory val Nothing
                                             liftTaskIO $ putMVar mps p
                                             cfg <- liftTask $ getConfig
                                             let cachefile = cfgPromisePrefix cfg++hashClosure clo
                                             liftTask $ diskify cachefile mps True
                              Nothing -> taskError $ "Failed looking up "++cloname++" in closure table"
             in do res <- ptry $ runTaskM tasker initialState :: ProcessM (Either SomeException (TaskState,()))
                   case res of
                     Left ex -> liftIO (putMVar mps (PromiseException (show ex))) >> throw ex
                     Right _ -> return ()

data NodeBossState = 
        NodeBossState
        {
           nsPromiseCache :: MVar (Map.Map PromiseId (MVar PromiseStorage)),
           nsRedeemerForwarding :: MVar (Map.Map PromiseId ProcessId)
        }

startNodeManager :: ProcessId -> ProcessM ()
startNodeManager masterpid = 
  let
      handler :: NodeBossState -> ProcessM a
      handler state = 
        let promisecache = nsPromiseCache state
            nmStart = roundtripResponse (\(NmStart promise clo queueing) -> 
               do promisestore <- liftIO $ newEmptyMVar
                  ret <- liftIO $ modifyMVar promisecache 
                    (\pc ->               let newpc = Map.insert promise promisestore pc 
                                           in return (newpc,True))
                  when (ret)
                       (startNodeWorker masterpid state promisestore clo)
                  return (NmStartResponse ret,state)) 
            nmTermination = matchProcessDown masterpid $
                     do forwardLogs Nothing
                        logS "TSK" LoInformation $ "Terminating nodeboss after my master "++show masterpid++" is gone"
                        terminate
            nmRedeem = roundtripResponseAsync (\(NmRedeem promise) ans -> 
                     let answerer = do pc <- liftIO $ readMVar promisecache
                                       case Map.lookup promise pc of
                                         Nothing -> ans NmRedeemResponseUnknown
                                         Just v -> do rv <- liftIO $ readMVar v -- possibly long wait
                                                      case rv of
                                                         PromiseInMemory rrv _ _ -> 
                                                                do liftIO $ modifyMVar_ v (\_ -> updatePromiseInMemory rv)
                                                                   ans (NmRedeemResponse rrv)
                                                         PromiseOnDisk fp -> do mpd <- undiskify fp v
                                                                                case mpd of
                                                                                   Nothing -> 
                                                                                     ans (NmRedeemResponseUnknown)
                                                                                   Just a ->
                                                                                     ans (NmRedeemResponse a)
                                                                                diskify fp v False
                                                         PromiseException _ -> ans NmRedeemResponseException
                      in do spawnLocal answerer
                            return state) False
         in receiveWait [nmStart, nmRedeem, nmTermination, matchUnknownThrow] >>= handler
   in do forwardLogs $ Just masterpid
         mypid <- getSelfPid
         monitorProcess mypid masterpid MaMonitor
         logS "TSK" LoInformation $ "Starting a nodeboss owned by " ++ show masterpid
         pc <- liftIO $ newMVar Map.empty
         pf <- liftIO $ newMVar Map.empty
         let initState = NodeBossState {nsPromiseCache=pc,nsRedeemerForwarding=pf}
         handler initState

-- | Starts a new context for executing a 'TaskM' environment.
-- The node on which this function is run becomes a new master
-- in a Task application; as a result, the application should
-- only call this function once. The master will attempt to
-- control all nodes that it can find; if you are going to be
-- running more than one CH application on a single network,
-- be sure to give each application a different network
-- magic (via cfgNetworkMagic). The master TaskM environment
-- created by this function can then spawn other threads,
-- locally or remotely, using 'newPromise' and friends.

runTask :: TaskM a -> ProcessM a
runTask = startMaster

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
              pf <- liftIO $ newMVar Map.empty
              let initialState = TaskState {tsMaster=master,tsNodeBoss=Just nodeboss,
                                            tsPromiseCache=pc, tsRedeemerForwarding=pf,
                                            tsMonitoring=Map.empty}
              res <- liftM snd $ runTaskM proc initialState
              liftIO $ putMVar mvdone res

type LocationSelector = MasterState -> ProcessM (NodeId,ProcessId)

selectLocation :: MasterState -> Locality -> ProcessM (Maybe (String,NodeId,ProcessId))
selectLocation ms locality =
   let nodes = msNodes ms
    in liftIO $ modifyMVar nodes
          (\n -> case n of
                    [] -> return (n,Nothing)
                    _ -> let dflt = (rotate n,Just $ head n)
                             filterify f = case filter f n of
                                             [] -> return dflt
                                             (a:_) -> return ((delete a n) ++ [a],Just a)
                          in  case cond locality of
                                LcUnrestricted -> return dflt
                                LcDefault -> return dflt
                                LcByRole l -> filterify (\(r,_,_) -> r `elem` l)
                                LcByNode l -> filterify (\(_,r,_) -> r `elem` l))
      where rotate [] = []
            rotate (h:t) = t ++ [h]
            cond l = case l of
                       LcDefault -> msDefaultLocality ms
                       _ -> l

countLocations :: MasterState -> ProcessM Int
countLocations ms = liftIO $ withMVar (msNodes ms) (\a -> return $ length a)

findPeers :: ProcessM [(String,NodeId)]
findPeers = liftM (concat . (map (\(role,v) -> [ (role,x) | x <- v] )) . Map.toList) getPeers

sendSilent :: (Serializable a) => ProcessId -> a -> ProcessM ()
sendSilent pid a = do res <- ptry $ send pid a
                      case res of
                        Left (TransmitException _) -> return ()
                        Right _ -> return ()

getStatus :: TaskM ()
getStatus = 
   do master <- getMaster
      res <- roundtrip master MmStatus
      case res of
        Left _ -> return ()
        Right (MmStatusResponse nodes promises) ->
              let verboseNodes = intercalate ", " (map show nodes)
                  verbosePromises = intercalate "\n" $ map (\(nb,l) -> (show nb)++" -- "++intercalate "," (map show l)) (Map.toList promises) 
               in tsay $ "\nKnown nodes: " ++ verboseNodes ++ "\n\nNodebosses: " ++ verbosePromises

runMaster :: (ProcessId -> ProcessM ()) -> ProcessM ProcessId
runMaster masterproc = 
  let
     probeOnce nodes seen masterpid =
        do recentlist <- findPeers -- TODO if a node fails to response to a probe even once, it's gone forever; be more flexible
           let newseen = seen `union` recentlist
           let topidlist = recentlist \\ seen
           let getnid (_,n,_) = n
           let cleanOut n = filter (\(_,nid,_) -> nid `elem` (map snd recentlist)) n
           newlypidded <- mapM (\(role,nid) -> 
                             do pid <- runWorkerNode masterpid nid
                                return (role,nid,pid)) topidlist           
           (newlist,totalseen) <- liftIO $ modifyMVar nodes (\oldlist ->
                        return ((cleanOut oldlist) ++ newlypidded,(recentlist,newseen)))
           let newlyadded = totalseen \\ seen
           mapM_ (\nid -> sendSilent masterpid (TmNewPeer nid)) (map snd newlyadded)
           return totalseen
     proberDelay = 10000000 -- how often do we check the network to see what nodes are available?
     prober nodes seen masterpid =
        do totalseen <- probeOnce nodes seen masterpid
           receiveTimeout proberDelay [matchUnknownThrow]
           prober nodes totalseen masterpid
     master state = 
        let 
            tryAlloc clo promiseid locality queueing =
              do ns <- selectLocation state locality
                 case ns of
                    Nothing -> do logS "TSK" LoCritical "Attempt to allocate a task, but no nodes found"
                                  return Nothing
                    Just (loc@(_,nid,nodeboss)) -> 
                         do res <- roundtripQuery PldUser nodeboss (NmStart promiseid clo queueing) -- roundtripQuery monitors and then unmonitors, which generates a lot of traffic; we probably don't need to do this
                            case res of
                              Left e -> 
                                 do logS "TSK" LoImportant $ "Failed attempt to start "++show clo++" on " ++show nid ++": "++show e
                                    return Nothing
                              Right (NmStartResponse True) -> return $ Just nodeboss
                              _ -> do logS "TSK" LoImportant $ "Failed attempt to start "++show clo++" on " ++show nid
                                      return Nothing
            basicAllocate clo promiseid locality queueing =
               do count <- countLocations state
                  res1 <- tryAlloc clo promiseid locality queueing
                  case res1 of 
                     Just _ -> return res1
                     Nothing ->  -- TODO we should try all matching locations before moving on to Unrestricted
                       do res <- stubborn count $ tryAlloc clo promiseid LcUnrestricted queueing
                          case res of
                             Nothing -> do logS "TSK" LoCritical $ "Terminally failed to start "++show clo
                                           return res
                             _ -> return res
            statusMsg = roundtripResponse
              (\x -> case x of
                        MmStatus -> 
                         do thenodes <- liftIO $ readMVar $ msNodes state
                            let knownNodes = map (\(_,n,_) -> n) thenodes
                                proctree = msAllocation state
                            return (MmStatusResponse knownNodes proctree,state))
            complainMsg = roundtripResponse
              (\x -> case x of
                       MmComplain procid promid -> 
                         case Map.lookup promid (msPromises state) of
                            Nothing -> return (MmComplainResponse nullPid,state) -- failure
                            Just (PromiseRecord curprocid curclo curlocality) 
                               | curprocid /= procid -> return (MmComplainResponse curprocid,state)
                               | otherwise -> 
                                   do res <- basicAllocate curclo promid curlocality defaultQueueing
                                      case res of
                                        Nothing -> return (MmComplainResponse nullPid,state) -- failure
                                        Just newprocid ->
                                           let newpromises = Map.insert promid (PromiseRecord newprocid curclo curlocality) (msPromises state)
                                            in return (MmComplainResponse newprocid,state {msPromises=newpromises})) 
            promiseMsg = roundtripResponse 
              (\x -> case x of
                       MmNewPromise clo locality queueing -> 
                          do 
                             let promiseid = msNextId state
                             res <- basicAllocate clo promiseid locality queueing
                             case res of
                                Just nodeboss -> 
                                        let newstate = state {msAllocation=newAllocation,msPromises=newPromises,msNextId=promiseid+1}
                                            newAllocation = Map.insertWith' (\a b -> nub $ a++b) nodeboss [promiseid] (msAllocation state)
                                            newPromises = Map.insert promiseid (PromiseRecord nodeboss clo locality) (msPromises state)
                                         in return (MmNewPromiseResponse nodeboss promiseid,newstate)
                                Nothing -> 
                                         return (MmNewPromiseResponseFail,state))
            simpleMsg = match 
              (\x -> case x of
                       TmNewPeer nid -> do logS "TSK" LoInformation $ "Found new peer " ++show nid
                                           return state)
         in receiveWait [simpleMsg, promiseMsg, complainMsg,statusMsg] >>= master -- TODO matchUnknownThrow
   in do nodes <- liftIO $ newMVar []
         selfnode <- getSelfNode
         selfpid <- getSelfPid
         let initState = MasterState {msNextId=0, msAllocation=Map.empty, msPromises=Map.empty, msNodes=nodes, msDefaultLocality = defaultLocality}
         masterpid <- spawnDaemonic (master initState)
         seennodes <- probeOnce nodes [] masterpid
         let getByNid _ [] = Nothing
             getByNid nid ((_,n,nodeboss):xs) = if nid==n then Just nodeboss else getByNid nid xs
         res <- liftIO $ withMVar nodes (\n -> return $ getByNid selfnode n)
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

-- TODO: setDefaultLocality :: Locality -> TaskM ()

-- | Like 'newPromise', but creates a promise whose
-- values is already known. In other words, it puts
-- a given, already-calculated value in a promise.
-- Conceptually (but not syntactically, due to closures),
-- you can consider it like this:
--
-- > toPromise a = newPromise (return a)
toPromise :: (Serializable a) => a -> TaskM (Promise a)
toPromise = toPromiseAt LcDefault

-- | A variant of 'toPromise' that lets the user
-- express a locality preference, i.e. some information
-- about which node will become the owner of the
-- new promise. These preferences will not necessarily
-- be respected.
toPromiseAt :: (Serializable a) => Locality -> a -> TaskM (Promise a)
toPromiseAt locality a = newPromiseAt locality (passthrough__closure a)

-- | Similar to 'toPromiseAt' and 'newPromiseNear'
toPromiseNear :: (Serializable a,Serializable b) => Promise b -> a -> TaskM (Promise a)
toPromiseNear (PromiseImmediate _) = toPromise
-- TODO should I consult tsRedeemerForwarding here?
toPromiseNear (PromiseBasic prhost prid) = toPromiseAt (LcByNode [nodeFromPid prhost])

-- | Creates an /immediate promise/, which is to say, a promise
-- in name only. Unlike a regular promise (created by 'toPromise'), 
-- this kind of promise contains the value directly. The 
-- advantage is that promise redemption is very fast, requiring
-- no network communication. The downside is that it the
-- underlying data will be copied along with the promise.
-- Useful only for small data.
toPromiseImm :: (Serializable a) => a -> TaskM (Promise a)
toPromiseImm = return . PromiseImmediate

-- | Given a function (expressed here as a closure, see "Remote.Call")
-- that computes a value, returns a token identifying that value.
-- This token, a 'Promise' can be moved about even if the
-- value hasn't been computed yet. The computing function 
-- will be started somewhere among the nodes visible to the
-- current master, preferring those nodes that correspond
-- to the 'defaultLocality'. Afterwards, attempts to
-- redeem the promise with 'readPromise' will contact the node
-- where the function is executing.
newPromise :: (Serializable a) => Closure (TaskM a) -> TaskM (Promise a)
newPromise = newPromiseAt LcDefault

-- | A variant of 'newPromise' that prefers to start
-- the computing function on the same node as the caller.
-- Useful if you plan to use the resulting value
-- locally.
newPromiseHere :: (Serializable a) => Closure (TaskM a) -> TaskM (Promise a)
newPromiseHere clo =
     do mynode <- liftTask $ getSelfNode
        newPromiseAt (LcByNode [mynode]) clo

-- | A variant of 'newPromise' that prefers to start
-- the computing function on the same node where some
-- other promise lives. The other promise is not
-- evaluated.
newPromiseNear :: (Serializable a, Serializable b) => Promise b -> Closure (TaskM a) -> TaskM (Promise a)
newPromiseNear (PromiseImmediate _) = newPromise
newPromiseNear (PromiseBasic prhost prid) = newPromiseAt (LcByNode [nodeFromPid prhost]) 

-- | A variant of 'newPromise' that prefers to start
-- the computing functions on some set of nodes that
-- have a given role (assigned by the cfgRole configuration
-- option).
newPromiseAtRole :: (Serializable a) => String -> Closure (TaskM a) -> TaskM (Promise a)
newPromiseAtRole role clo = newPromiseAt (LcByRole [role]) clo

-- | A variant of 'newPromise' that lets the user
-- specify a 'Locality'. The other flavors of newPromise,
-- such as 'newPromiseAtRole', 'newPromiseNear', and
-- 'newPromiseHere' at just shorthand for a call to this function.
newPromiseAt :: (Serializable a) => Locality -> Closure (TaskM a) -> TaskM (Promise a)
newPromiseAt locality clo = 
   let realclo = makePayloadClosure clo
    in case realclo of
          Just plclo -> do master <- getMaster
                           res <- roundtrip master (MmNewPromise plclo locality defaultQueueing)
                           case res of
                              Right (MmNewPromiseResponse pid prid) -> return $ PromiseBasic pid prid
                              Right (MmNewPromiseResponseFail) ->
                                          taskError $ "Spawning of closure "++show clo++" by newPromise failed"
                              Left tms -> taskError $ "Spawning of closure "++show clo++" by newPromise resulted in "++show tms
          Nothing -> taskError $ "The specified closure, "++show clo++", can't produce payloads"

-- | Given a promise, gets the value that is being
-- calculated. If the calculation has finished,
-- the owning node will be contacted and the data
-- moved to the current node. If the calculation
-- has not finished, this function will block
-- until it has. If the calculation failed
-- by throwing an exception (e.g. divide by zero),
-- then this function will throw an excption as well
-- (a 'TaskException'). If the node owning the
-- promise is not accessible, the calculation
-- will be restarted.
readPromise :: (Serializable a) => Promise a -> TaskM a
readPromise (PromiseImmediate a) = return a
readPromise thepromise@(PromiseBasic prhost prid) = 
       do mp <- lookupCachedPromise prid
          case mp of
            Nothing -> do fprhost <- liftM (maybe prhost id) $ lookupForwardedRedeemer prid
                          res <- roundtrip fprhost (NmRedeem prid)
                          case res of
                             Left e -> do tlogS "TSK" LoInformation $ "Complaining about promise " ++ show prid ++" on " ++show fprhost++" because of "++show e
                                          complain prhost fprhost prid
                             Right NmRedeemResponseUnknown -> 
                                       do tlogS "TSK" LoInformation $ "Complaining about promise " ++ show prid ++" on " ++show fprhost++" because allegedly unknown"
                                          complain prhost fprhost prid
                             Right (NmRedeemResponse thedata) -> 
                                                  do extracted <- extractFromPayload thedata
                                                     promiseinmem <- liftTaskIO $ makePromiseInMemory thedata (Just $ toDyn extracted)
                                                     putPromiseInCache prid promiseinmem
                                                     return extracted
                             Right NmRedeemResponseException ->
                                  taskError "Failed promise redemption" -- don't redeem, this is a terminal failure
            Just mv -> do val <- liftTaskIO $ readMVar mv -- possible long wait here
                          case val of -- TODO this read/write MVars should be combined!
                             PromiseInMemory v utc thedyn -> 
                                     case thedyn of
                                       Just thedynvalue -> 
                                         case fromDynamic thedynvalue of
                                           Nothing -> do liftTask $ logS "TSK" LoStandard "Insufficiently dynamic promise cache"
                                                         extractFromPayload v
                                           Just realval -> do updated <- liftTaskIO $ makePromiseInMemory v thedyn
                                                              putPromiseInCache prid updated
                                                              return realval
                                       Nothing -> do extracted <- extractFromPayload v
                                                     updated <- liftTaskIO $ makePromiseInMemory v (Just $ toDyn extracted)
                                                     putPromiseInCache prid updated
                                                     return extracted
                             PromiseException _ -> taskError $ "Redemption of promise failed"
                             PromiseOnDisk fp -> do mpd <- liftTask $ undiskify fp mv
                                                    liftTask $ spawnLocal $ diskify fp mv False
                                                    case mpd of
                                                       Just dat -> extractFromPayload dat
                                                       _ -> taskError "Promise extraction from disk failed"
     where extractFromPayload v = do out <- liftTaskIO $ serialDecode v
                                     case out of
                                       Just r -> return r
                                       Nothing -> taskError "Unexpected payload type"
           complain prhost fprhost prid =
                     do master <- getMaster
                        response <- roundtrip master (MmComplain fprhost prid)
                        case response of 
                          Left a -> taskError $ "Couldn't file complaint with master about " ++ show fprhost ++ " because " ++ show a
                          Right (MmComplainResponse newhost) 
                            | newhost == nullPid -> taskError $ "Couldn't file complaint with master about " ++ show fprhost
                            | otherwise -> do setForwardedRedeemer prid newhost
                                              readPromise thepromise

data TaskState = TaskState
      {
         tsMaster :: ProcessId,
         tsNodeBoss :: Maybe ProcessId,
         tsPromiseCache :: MVar (Map.Map PromiseId (MVar PromiseStorage)),
         tsRedeemerForwarding :: MVar (Map.Map PromiseId ProcessId),
         tsMonitoring :: Map.Map ProcessId ()
      }

data TaskM a = TaskM { runTaskM :: TaskState -> ProcessM (TaskState, a) } deriving (Typeable)

instance Monad TaskM where
   m >>= k = TaskM $ \ts -> do
                (ts',a) <- runTaskM m ts
                (ts'',a') <- runTaskM (k a) (ts')
                return (ts'',a')              
   return x = TaskM $ \ts -> return $ (ts,x)

lookupForwardedRedeemer :: PromiseId -> TaskM (Maybe ProcessId)
lookupForwardedRedeemer q = 
   TaskM $ \ts ->
     liftIO $ withMVar (tsRedeemerForwarding ts) $ (\fwd ->
       let lo = Map.lookup q fwd 
        in return (ts,lo))

setForwardedRedeemer :: PromiseId -> ProcessId -> TaskM ()
setForwardedRedeemer from to =
   TaskM $ \ts -> liftIO $ modifyMVar (tsRedeemerForwarding ts)  (\fwd ->
     let newmap = Map.insert from to fwd 
      in return ( newmap,(ts,())  ) )

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

remoteCallRectify :: (Serializable a) => ProcessM (TaskM a) -> TaskM Payload
remoteCallRectify x = 
         do a <- liftTask x
            res <- a
            liftTaskIO $ serialEncode res

-- | A Task-monadic version of 'Remote.Process.say'.
-- Puts text messages in the log.
tsay :: String -> TaskM ()
tsay a = liftTask $ say a

-- | Writes various kinds of messages to the
-- "Remote.Process" log.
tlogS :: LogSphere -> LogLevel -> String -> TaskM ()
tlogS a b c = liftTask $ logS a b c

----------------------------------------------
-- * MapReduce
----------------------------------------------

-- | A data structure that stores the important
-- user-provided functions that are the namesakes
-- of the MapReduce algorithm.
-- The number of mapper processes can be controlled
-- by the user by controlling the length of the string
-- returned by mtChunkify. The number of reducer
-- promises is controlled by the number of values
-- values returned by shuffler.
-- The user must provide their own mapper and reducer.
-- For many cases, the default chunkifier ('chunkify')
-- and shuffler ('shuffle') are adequate.
data MapReduce rawinput input middle1 middle2 result 
    = MapReduce
      {
        mtMapper :: input -> Closure (TaskM [middle1]),
        mtReducer :: middle2 -> Closure (TaskM result),
        mtChunkify :: rawinput -> [input],
        mtShuffle :: [middle1] -> [middle2]
      }

-- | A convenient way to provide the 'mtShuffle' function
-- as part of 'mapReduce'. 
shuffle :: Ord a => [(a,b)] -> [(a,[b])]
shuffle q = 
    let semi = groupBy (\(a,_) (b,_) -> a==b) (sortBy (\(a,_) (b,_) -> compare a b) q)
     in map (\x -> (fst $ head x,map snd x)) semi 

-- | A convenient way to provide the 'mtChunkify' function
-- as part of 'mapReduce'. 
chunkify :: Int -> [a] -> [[a]] 
chunkify numChunks l 
  | numChunks <= 0 = taskError "Can't chunkify into less than one chunk"
  | otherwise = splitSize (ceiling $ fromIntegral (length l) / fromIntegral numChunks) l
   where
      splitSize _ [] = []
      splitSize i v = let (first,second) = splitAt i v 
                       in first : splitSize i second

-- | The MapReduce algorithm, implemented in a very
-- simple form on top of the Task layer. Its
-- use depends on four user-determined data types:
--
-- * input -- The data type provided as the input to the algorithm as a whole and given to the mapper.
--
-- * middle1 -- The output of the mapper. This may include some /key/ which is used by the shuffler to allocate data to reducers.
-- If you use the default shuffler, 'shuffle', this type must have the form @Ord a => (a,b)@.
--
-- * middle2 -- The output of the shuffler. The default shuffler emits a type in the form @Ord => (a,[b])@. Each middle2 output 
-- by shuffler is given to a separate reducer.
--
-- * result -- The output of the reducer, upon being given a bunch of middles.
mapReduce :: (Serializable i,Serializable k,Serializable m,Serializable r) =>
             MapReduce ri i k m r -> ri -> TaskM [r]
mapReduce mr inputs =
    let chunks = (mtChunkify mr) inputs 
     in do 
           pmapResult <- mapM (\chunk -> 
                 newPromise ((mtMapper mr) chunk) ) chunks
           mapResult <- mapM readPromise pmapResult
           let shuffled = (mtShuffle mr) (concat mapResult)
           pres <- mapM (\mid2 -> 
                  newPromise ((mtReducer mr) mid2)) shuffled
           mapM readPromise pres

