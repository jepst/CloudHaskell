{-# LANGUAGE DeriveDataTypeable #-}

-- | This module is the core of Cloud Haskell. It provides 
-- processes, messages, monitoring, and configuration.
module Remote.Process  (
                       -- * The Process monad
                       ProcessM,
                       NodeId,ProcessId,
                       PeerInfo,
                       nullPid,getSelfPid,getSelfNode,isPidLocal,

                       -- * Message receiving
                       expect,
                       MatchM,receive,receiveWait,receiveTimeout,
                       match,matchIf,matchCond,matchUnknown,matchUnknownThrow,matchProcessDown,

                       -- * Message sending
                       send,sendQuiet,

                       -- * Logging functions
                       logS,say,
                       LogSphere,LogLevel(..),LogTarget(..),LogFilter(..),LogConfig(..),
                       setLogConfig,getLogConfig,setNodeLogConfig,setRemoteNodeLogConfig,defaultLogConfig,

                       -- * Exception handling
                       ptimeout,
                       UnknownMessageException(..),ServiceException(..),
                       TransmitException(..),TransmitStatus(..),

                       -- * Process naming
                       nameSet, nameQuery, nameQueryOrStart,

                       -- * Process spawning and monitoring
                       spawnLocal,spawnLocalAnd,forkProcess,spawn,spawnAnd,spawnLink,unpause,
                       AmSpawnOptions(..),defaultSpawnOptions,MonitorAction(..),SignalReason(..),
                       ProcessMonitorException(..),linkProcess,monitorProcess,unmonitorProcess,withMonitor,pingNode,
                       callRemote,callRemotePure,callRemoteIO,
                       terminate,

                       -- * Config file
                       readConfig,emptyConfig,Config(..),getConfig, getCfgArgs,

                       -- * Initialization
                       initNode,roleDispatch,setDaemonic,
                       waitForThreads,performFinalization,forkAndListenAndDeliver,runLocalProcess,

                       -- * Closures
                       makeClosure,invokeClosure,evaluateClosure,
 
                       -- * Debugging aids
                       getQueueLength,nodeFromPid,localFromPid,hostFromNid,

                       -- * Various internals, not for general use
                       PortId,LocalProcessId,
                       localRegistryHello,localRegistryRegisterNode, localRegistryQueryNodes,localRegistryUnregisterNode,
                       sendSimple,makeNodeFromHost,getNewMessageLocal, getProcess,Message,Process,prNodeRef,
                       roundtripResponse,roundtripResponseAsync,roundtripQuery,roundtripQueryMulti,
                       makePayloadClosure,getLookup,diffTime,roundtripQueryImpl,roundtripQueryUnsafe,
                       PayloadDisposition(..),suppressTransmitException,Node,getMessagePayload,getMessageType,

                       -- * System service processes, not for general use
                       startSpawnerService, startLoggingService, startProcessMonitorService, startLocalRegistry, startFinalizerService,
                       startNodeMonitorService, startProcessRegistryService, standaloneLocalRegistry
                       )
                       where

import qualified Prelude as Prelude
import Prelude hiding (catch, id, init, last, lookup, pi)

import Control.Concurrent (forkIO,ThreadId,threadDelay)
import Control.Concurrent.MVar (MVar,newMVar, newEmptyMVar,takeMVar,putMVar,modifyMVar,modifyMVar_,readMVar)
import Control.Exception (ErrorCall(..),throwTo,bracket,try,Exception,throw,evaluate,finally,SomeException,catch)
import Control.Monad (foldM,when,liftM,forever)
import Control.Monad.Trans (MonadIO,liftIO)
import Control.Monad.IO.Control (MonadControlIO, liftControlIO)
import qualified Control.Exception.Control as Control (try, bracket, finally)
import Data.Functor ((<$>))
import Data.Binary (Binary,put,get,putWord8,getWord8)
import Data.Char (isSpace,isDigit)
import Data.List (isSuffixOf,foldl', isPrefixOf)
import Data.Maybe (catMaybes,isNothing)
import Data.Typeable (Typeable)
import Data.Data (Data)
import Data.Unique (newUnique,hashUnique)
import System.IO (Handle,hClose,hSetBuffering,hGetChar,hPutChar,BufferMode(..),hFlush)
import System.IO.Error (isEOFError,isDoesNotExistError,isUserError)
import Network.BSD (getHostName)
import Network (HostName,PortID(..),listenOn,accept,sClose,connectTo)
import Network.Socket (setSocketOption,SocketOption(..),socketPort,aNY_PORT )
import qualified Data.Map as Map (Map,keys,fromList,unionWith,elems,singleton,member,update,empty,adjust,alter,insert,delete,lookup,toList,size,insertWith')
import Remote.Reg (getEntryByIdent,Lookup,empty)
import Remote.Encoding (serialEncode,serialDecode,serialEncodePure,serialDecodePure,dynamicEncodePure,dynamicDecodePure,DynamicPayload,Payload,Serializable,hPutPayload,hGetPayload,getPayloadType,getDynamicPayloadType)
import System.Environment (getArgs)
import qualified System.Timeout (timeout)
import Data.Time (toModifiedJulianDay,Day(..),picosecondsToDiffTime,getCurrentTime,diffUTCTime,UTCTime(..),utcToLocalZonedTime)
import Remote.Closure (Closure (..))
import Control.Concurrent.STM (STM,atomically,retry,orElse)
import Control.Concurrent.STM.TChan (TChan,isEmptyTChan,readTChan,newTChanIO,writeTChan)
import Control.Concurrent.Chan (newChan,readChan,writeChan)
import Control.Concurrent.STM.TVar (TVar,newTVarIO,readTVar,writeTVar)
import Control.Concurrent.QSem (QSem,newQSem,waitQSem,signalQSem)
import Data.IORef (IORef,newIORef,readIORef,writeIORef)
import Data.Fixed (Pico)

----------------------------------------------
-- * Process monad
----------------------------------------------

type PortId = Int
type LocalProcessId = Int

-- | The Config structure encapsulates the user-settable configuration options for each node.
-- This settings are usually read in from a configuration file or from the executable's
-- command line; in either case, see 'Remote.Init.remoteInit' and 'readConfig'
data Config = Config 
         {
             cfgRole :: !String, -- ^ The user-assigned role of this node determines what its initial behavior is and how it presents itself to its peers. Default to NODE
             cfgHostName :: !HostName, -- ^ The hostname, used as a basis for creating the name of the node. If unspecified, the OS will be queried. Since the hostname is part of the nodename, the computer must be accessible to other nodes using this name.
             cfgListenPort :: !PortId, -- ^ The TCP port on which to listen to for new connections. If unassigned or 0, the OS will assign a free port.
             cfgLocalRegistryListenPort :: !PortId, -- ^ The TCP port on which to communicate with the local node registry, or to start the local node registry if it isn't already running. This defaults to 38813 and shouldn't be changed unless you have prohibitive firewall rules
             cfgPeerDiscoveryPort :: !PortId, -- ^ The UDP port on which local peer discovery broadcasts are sent. Defaults to 38813, and only matters if you rely on dynamic peer discovery
             cfgNetworkMagic :: !String, -- ^ The unique identifying string for this network or application. Must not contain spaces. The uniqueness of this string ensures that multiple applications running on the same physical network won't accidentally communicate with each other. All nodes of your application should have the same network magic. Defaults to MAGIC
             cfgKnownHosts :: ![String], -- ^ A list of hosts where nodes may be running. When 'Remote.Peer.getPeers' or 'Remote.Peer.getPeerStatic' is called, each host on this list will be queried for its nodes. Only matters if you rely on static peer discovery.
             cfgRoundtripTimeout :: !Int, -- ^ Microseconds to wait for a response from a system service on a remote node. If your network has high latency or congestion, you may need to increase this to avoid incorrect reports of node inaccessibility. 0 to wait indefinitely (not recommended).
             cfgMaxOutgoing :: !Int, -- ^ A limit on the number of simultaneous outgoing connections per node
             cfgPromiseFlushDelay :: !Int, -- ^ Time in microseconds before an in-memory promise is flushed to disk. 0 to disable disk flush entirely.
             cfgPromisePrefix :: !String, -- ^ Prepended to the filename of flushed promises.
             cfgArgs :: [String] -- ^ Command-line arguments that are not part of the node configuration are placed here and can be examined by your application
--             logConfig :: LogConfig
         } deriving (Show)

type ProcessTable = Map.Map LocalProcessId ProcessTableEntry

type AdminProcessTable = Map.Map ServiceId LocalProcessId

data Node = Node
       {
--           ndNodeId :: NodeId
           ndProcessTable :: ProcessTable,
           ndAdminProcessTable :: AdminProcessTable,
--           connectionTable :: Map.Map HostName Handle -- outgoing connections only
--           ndHostEntries :: [HostEntry],
           ndHostName :: HostName,
           ndListenPort :: PortId,
           ndConfig :: Config,
           ndLookup :: Lookup,
           ndLogConfig :: LogConfig,
           ndNodeFinalizer :: MVar (),
           ndNodeFinalized :: MVar (),
           ndOutgoing :: QSem,
           ndNextProcessId :: LocalProcessId
           --conncetion table -- each one is MVared
           --  also contains time info about last contact
       }

-- | Identifies a node somewhere on the network. These
-- can be queried from 'getPeers'. See also 'getSelfNode'
data NodeId = NodeId !HostName !PortId deriving (Typeable,Eq,Ord,Data)

-- | Identifies a process somewhere on the network. These
-- are produced by the 'spawn' family of functions and
-- consumed by 'send'. When a process ends, its process ID
-- ceases to be valid. See also 'getSelfPid'
data ProcessId = ProcessId !NodeId !LocalProcessId deriving (Typeable,Eq,Ord,Data)

instance Binary NodeId where
   put (NodeId h p) = put h >> put p
   get = get >>= \h -> 
         get >>= \p -> 
         return (NodeId h p)

instance Show NodeId where
   show (NodeId hostname portid) = concat ["nid://",hostname,":",show portid,"/"]

instance Read NodeId where
   readsPrec _ s = if isPrefixOf "nid://" s
                       then let a = drop 6 s
                                hostname = takeWhile ((/=) ':') a
                                b = drop (length hostname+1) a
                                port= takeWhile ((/=) '/') b
                                c = drop (length port+1) b
                                result = NodeId hostname (read port) in
                                if (not.null) hostname && (not.null) port
                                   then [(result,c)]
                                   else error "Bad parse looking for NodeId"
                       else error "Bad parse looking for NodeId"

instance Binary ProcessId where
   put (ProcessId n p) = put n >> put p
   get = get >>= \n -> 
         get >>= \p -> 
         return (ProcessId n p)

instance Show ProcessId where
   show (ProcessId (NodeId hostname portid) localprocessid) = concat ["pid://",hostname,":",show portid,"/",show localprocessid,"/"]

instance Read ProcessId where
   readsPrec _ s = if isPrefixOf "pid://" s
                       then let a = drop 6 s
                                hostname = takeWhile ((/=) ':') a
                                b = drop (length hostname+1) a
                                port= takeWhile ((/=) '/') b
                                c = drop (length port+1) b
                                pid = takeWhile ((/=) '/') c
                                d = drop (length pid+1) c
                                result = ProcessId (NodeId hostname (read port)) (read pid) in
                                if (not.null) hostname && (not.null) pid && (not.null) port
                                   then [(result,d)]
                                   else error "Bad parse looking for ProcessId"
                       else error "Bad parse looking for ProcessId"

data PayloadDisposition = PldUser |
                          PldAdmin
                          deriving (Typeable,Read,Show,Eq)

data Message = 
   EncodedMessage { msgEDisposition :: PayloadDisposition, msgEHeader :: Maybe Payload, msgEPayload :: !Payload }
 | DynamicMessage { msgDDisposition :: PayloadDisposition, msgDHeader :: Maybe DynamicPayload, msgDPayload :: DynamicPayload }

makeMessage :: (Serializable msg, Serializable hdr) => Bool -> PayloadDisposition -> msg -> Maybe hdr -> Message
makeMessage True pld msg hdr = 
   DynamicMessage {msgDDisposition=pld,msgDHeader=maybe Nothing (Just . dynamicEncodePure) hdr, msgDPayload=dynamicEncodePure msg}
makeMessage False pld msg hdr = 
   EncodedMessage {msgEDisposition=pld,msgEHeader=maybe Nothing (Just . serialEncodePure) hdr,msgEPayload=serialEncodePure msg}

getMessageType :: Message -> String
getMessageType (EncodedMessage _ _ a) = getPayloadType a
getMessageType (DynamicMessage _ _ a) = getDynamicPayloadType a

getMessagePayload :: (Serializable a) => Message -> Maybe a
getMessagePayload (EncodedMessage _ _ a) = serialDecodePure a
getMessagePayload (DynamicMessage _ _ a) = dynamicDecodePure a

{- UNUSED
messageHasHeader :: Message -> Bool
messageHasHeader (EncodedMessage _ (Just _) _) = True
messageHasHeader (DynamicMessage _ (Just _) _) = True
messageHasHeader _ = False
-}

getMessageHeader :: (Serializable a) => Message -> Maybe a
getMessageHeader (EncodedMessage _ a _) = maybe Nothing serialDecodePure a
getMessageHeader (DynamicMessage _ a _) = maybe Nothing dynamicDecodePure a

getMessageDisposition :: Message -> PayloadDisposition
getMessageDisposition (EncodedMessage a _ _) = a
getMessageDisposition (DynamicMessage a _ _) = a

data ProcessTableEntry = ProcessTableEntry
  {
      pteThread :: ThreadId,
      pteChannel :: TChan Message,
      pteDeathT :: TVar Bool,
      pteDeath :: MVar (),
      pteDaemonic :: Bool
  }

data ProcessState = ProcessState
   {
       prQueue :: Queue Message
   }

data Process = Process
   {
       prPid :: ProcessId,
       prSelf :: LocalProcessId,
       prNodeRef :: MVar Node,
       prThread :: ThreadId,
       prChannel :: TChan Message,
       prState :: TVar ProcessState,
       prPool :: IORef (Map.Map NodeId Handle),
       prLogConfig :: Maybe LogConfig
   } 
                           
-- | The monad ProcessM is the core of the process layer. Functions
-- in the ProcessM monad may participate in messaging and create
-- additional concurrent processes. You can create
-- a ProcessM context from an 'IO' context with the 'remoteInit' function.
data ProcessM a = ProcessM {runProcessM :: Process -> IO (Process,a)} deriving Typeable

instance Monad ProcessM where
    m >>= k = ProcessM $ (\p -> (runProcessM m) p >>= (\(news,newa) -> runProcessM (k newa) news))
    return x = ProcessM $ \s -> return (s,x)

instance Functor ProcessM where
    fmap f v = ProcessM $ (\p -> (runProcessM v) p >>= (\x -> return $ fmap f x))

instance MonadIO ProcessM where
    liftIO arg = ProcessM $ \pr -> (arg >>= (\x -> return (pr,x)))

instance MonadControlIO ProcessM where
    liftControlIO f = ProcessM $ \p ->
      let runInIO m = (\t -> ProcessM $ \_ -> return t) <$> runProcessM m p
      in f runInIO >>= \x -> return (p, x)

getProcess :: ProcessM (Process)
getProcess = ProcessM $ \x -> return (x,x)

-- | Returns command-line arguments provided to the
-- executable, excluding any command line arguments
-- that were processed by the framework.
getCfgArgs :: ProcessM [String]
getCfgArgs = do cfg <- getConfig
                return $ cfgArgs cfg

getConfig :: ProcessM (Config)
getConfig = do p <- getProcess
               node <- liftIO $ readMVar (prNodeRef p)
               return $ ndConfig node

getConfigI :: MVar Node -> IO Config
getConfigI mnode = do node <- readMVar mnode
                      return $ ndConfig node

getLookup :: ProcessM (Lookup)
getLookup = do p <- getProcess
               node <- liftIO $ readMVar (prNodeRef p)
               return $ ndLookup node

putProcess :: Process -> ProcessM ()
putProcess p = ProcessM $ \_ -> return (p,())

----------------------------------------------
-- * Message pattern matching
----------------------------------------------

-- | This monad provides the state and structure for
-- matching received messages from the incoming message queue.
-- It's the interface between the 'receive' family of functions,
-- and the 'match' family, which together can express which
-- messages can be accepted.
data MatchM q a = MatchM { runMatchM :: MatchBlock -> STM ((MatchBlock,Maybe (ProcessM q)),a) }

instance Monad (MatchM q) where
    m >>= k = MatchM $ \mbi -> do
                (mb,a) <- runMatchM m mbi
                (mb',a2) <- runMatchM (k a) (fst mb)
                return (mb',a2)
                 
    return x = MatchM $ \mb -> return $ ((mb,Nothing),x)
--    fail _ = MatchM $ \_ -> return (False,Nothing)

returnHalt :: a -> ProcessM q -> MatchM q a
returnHalt x invoker = MatchM $ \mb -> return $ ((mb,Just (invoker)),x)

liftSTM :: STM a -> MatchM q a
liftSTM arg = MatchM $ \mb -> do a <- arg 
                                 return ((mb,Nothing),a)


data MatchBlock = MatchBlock
     {
        mbMessage :: Message
     }

getMatch :: MatchM q MatchBlock
getMatch = MatchM $ \x -> return ((x,Nothing), x)


getNewMessage :: Process -> STM Message
getNewMessage p = readTChan $ prChannel p

getNewMessageLocal :: Node -> LocalProcessId -> STM (Maybe Message)
getNewMessageLocal node lpid = do mpte <- getProcessTableEntry (node) lpid
                                  case mpte of
                                     Just pte -> do 
                                                    msg <- readTChan (pteChannel pte)
                                                    return $ Just msg
                                     Nothing -> return Nothing
                      
getQueueLength :: ProcessM Int
getQueueLength = 
  do p <- getProcess
     liftIO $ atomically $ 
          do q <- getCurrentMessages p
             return $ length q

getCurrentMessages :: Process -> STM [Message]
getCurrentMessages p = do
                         msgs <- cleanChannel (prChannel p) []
                         ps <- readTVar (prState p)
                         let q = (prQueue ps) 
                         let newq = queueInsertMulti q msgs
                         writeTVar (prState p) ps {prQueue = newq}
                         return $ queueToList newq
     where cleanChannel c m = do isEmpty <- isEmptyTChan c
                                 if isEmpty 
                                    then return m
                                    else do item <- readTChan c
                                            cleanChannel c (item:m)
                        
matchMessage :: [MatchM q ()] -> Message -> STM (Maybe (ProcessM q))
matchMessage matchers msg = do (_mb,r) <- (foldl orElse (retry) (map executor matchers)) `orElse` (return (theMatchBlock,Nothing))
                               return r
   where executor x = do 
                         (ok@(_mb,matchfound),_) <- runMatchM x theMatchBlock
                         case matchfound of
                            Nothing -> retry
                            _ -> return ok
         theMatchBlock = MatchBlock {mbMessage = msg}

matchMessages :: [MatchM q ()] -> [(Message,STM ())] -> STM (Maybe (ProcessM q))
matchMessages matchers msgs = (foldl orElse (retry) (map executor msgs)) `orElse` (return Nothing)
   where executor (msg,acceptor) = do
                                      res <- matchMessage matchers msg
                                      case res of
                                         Nothing -> retry
                                         Just pmq -> acceptor >> return (Just pmq)

-- | Examines the message queue of the current process, matching each message against each of the
-- provided message pattern clauses (typically provided by a function from the 'match' family). If
-- a message matches, the corresponding handler is invoked and its result is returned. If no
-- message matches, Nothing is returned.
receive :: [MatchM q ()] -> ProcessM (Maybe q)
receive m = do p <- getProcess
               res <- convertErrorCall $ liftIO $ atomically $ 
                          do
                             msgs <- getCurrentMessages p
                             matchMessages m (messageHandlerGenerator (prState p) msgs)
               case res of
                  Nothing -> return Nothing
                  Just n -> do q <- n
                               return $ Just q

-- | A simple way to receive messages. 
-- This will return the first message received
-- of the specified type; if no such message
-- is available, the function will block.
-- Unlike the 'receive' family of functions,
-- this function does not allow the notion
-- of choice in message extraction.
expect :: (Serializable a) => ProcessM a
expect = receiveWait [match return]

-- | Examines the message queue of the current process, matching each message against each of the
-- provided message pattern clauses (typically provided by a function from the 'match' family). If
-- a message matches, the corresponding handler is invoked and its result is returned. If no
-- message matches, the function blocks until a matching message is received.
receiveWait :: [MatchM q ()] -> ProcessM q
receiveWait m = do f <- receiveWaitImpl m
                   f


-- | Examines the message queue of the current process, matching each message against each of the
-- provided message pattern clauses (typically provided by a function from the 'match' family). If
-- a message matches, the corresponding handler is invoked and its result is returned. If no
-- message matches, the function blocks until a matching message is received, or until the
-- specified time in microseconds has elapsed, at which point it will return Nothing.
-- If the specified time is 0, this function is equivalent to 'receive'.
receiveTimeout :: Int -> [MatchM q ()] -> ProcessM (Maybe q)
receiveTimeout 0 m = receive m
receiveTimeout to m | to > 0 = 
          do res <- ptimeout to $ receiveWaitImpl m
             case res of
               Nothing -> return Nothing
               Just f -> do q <- f
                            return $ Just q

messageHandlerGenerator :: TVar ProcessState -> [Message] -> [(Message, STM ())]
messageHandlerGenerator prSt msgs = 
     map (\(m,q) -> (m,do ps <- readTVar prSt
                          writeTVar prSt ps {prQueue = queueFromList q})) (exclusionList msgs)


receiveWaitImpl :: [MatchM q ()] -> ProcessM (ProcessM q)
receiveWaitImpl m = 
            do p <- getProcess
               v <- attempt1 p
               attempt2 p v
      where 
            attempt2 p v = 
                case v of
                  Just n -> return n
                  Nothing -> do ret <- convertErrorCall $ liftIO $ atomically $ 
                                         do 
                                            msg <- getNewMessage p
                                            ps <- readTVar (prState p)
                                            let oldq = prQueue ps
                                            writeTVar (prState p) ps {prQueue = queueInsert oldq msg}
                                            matchMessages m [(msg,writeTVar (prState p) ps)]
                                attempt2 p ret
            attempt1 p = convertErrorCall $ liftIO $ atomically $ 
                          do
                             msgs <- getCurrentMessages p
                             matchMessages m (messageHandlerGenerator (prState p) msgs)

convertErrorCall :: ProcessM a -> ProcessM a
convertErrorCall f =
   do a <- Control.try ff
      case a of
        Right c -> return c
        Left b -> throw $ TransmitException $ QteOther $ show (b::ErrorCall)
  where ff = do q <- f
                q `seq` return q

{- UNUSED
matchDebug :: (Message -> ProcessM q) -> MatchM q ()
matchDebug f = do mb <- getMatch
                  returnHalt () (f (mbMessage mb))
-}

-- | A catch-all variant of 'match' that invokes user-provided code and
-- will extact any message from the queue. This is useful for matching
-- against messages that are not recognized. Since message matching patterns
-- are evaluated in order, this function, if used, should be the last element
-- in the list of matchers given to 'receiveWait' and similar functions.
matchUnknown :: ProcessM q -> MatchM q ()
matchUnknown body = returnHalt () body

-- | A variant of 'matchUnknown' that throws a 'UnknownMessageException'
-- if the process receives a message that isn't extracted by another message matcher.
-- Equivalent to:
--
-- > matchUnknown (throw (UnknownMessageException "..."))
matchUnknownThrow :: MatchM q ()
matchUnknownThrow = do mb <- getMatch
                       returnHalt () (throw $ UnknownMessageException (getMessageType (mbMessage mb)))

-- | Used to specify a message pattern in 'receiveWait' and related functions.
-- Only messages containing data of type /a/, where /a/ is the argument to the user-provided
-- function in the first parameter of 'match', will be removed from the queue, at which point
-- the user-provided function will be invoked.
match :: (Serializable a) => (a -> ProcessM q) -> MatchM q ()
match = matchCoreHeaderless  (const True)

-- | Similar to 'match', but allows for additional criteria to be checked prior to message acceptance.
-- Here, the first user-provided function operates as a filter, and the message will be accepted
-- only if it returns True. Once it's been accepted, the second user-defined function is invoked,
-- as in 'match'
matchIf :: (Serializable a) => (a -> Bool) -> (a -> ProcessM q) -> MatchM q ()
matchIf = matchCoreHeaderless 

matchCond :: (Serializable a) => (a -> Maybe (ProcessM q)) -> MatchM q ()
matchCond f = 
   matchIf (not . isNothing . f) run
  where run a = case f a of
                   Nothing -> throw $ TransmitException $ QteOther $ "Indecesive predicate in matchCond"
                   Just q -> q 

matchCoreHeaderless :: (Serializable a) => (a -> Bool) -> (a -> ProcessM q) -> MatchM q ()
matchCoreHeaderless f g = matchCore (\(a,b) -> b==(Nothing::Maybe ()) && f a)
                                            (\(a,_) -> g a)

matchCore :: (Serializable a,Serializable b) => ((a,Maybe b) -> Bool) -> ((a,Maybe b) -> ProcessM q) -> MatchM q ()
matchCore cond body = 
        do mb <- getMatch
           doit mb
 where 
  doit mb = 
        let    
           decodified = getMessagePayload (mbMessage mb)
           decodifiedh = getMessageHeader (mbMessage mb)
        in -- decodified `seq` decodifiedh `seq`          TODO is this good or bad?
            case decodified of
              Just x -> if cond (x,decodifiedh) 
                           then returnHalt () (body (x,decodifiedh))
                           else liftSTM retry
              Nothing -> liftSTM retry



----------------------------------------------
-- * Exceptions and return values
----------------------------------------------

-- | Thrown in response to a bad configuration
-- file or command line option, most likely
-- in 'Config'
data ConfigException = ConfigException String deriving (Show,Typeable)
instance Exception ConfigException

-- | Thrown by various network-related functions when
-- communication with a host has failed
data TransmitException = TransmitException TransmitStatus deriving (Show,Typeable)
instance Exception TransmitException

-- | Thrown by 'matchUnknownThrow' in response to a message
-- of a wrong type being received by a process
data UnknownMessageException = UnknownMessageException String deriving (Show,Typeable)
instance Exception UnknownMessageException

-- | Thrown by "Remote.Process" system services in response
-- to some problem
data ServiceException = ServiceException String deriving (Show,Typeable)
instance Exception ServiceException

-- | Used internally by 'terminate' to mark the orderly termination
-- of a process
data ProcessTerminationException = ProcessTerminationException deriving (Show,Typeable)
instance Exception ProcessTerminationException

data TransmitStatus = QteOK
                    | QteUnknownPid
                    | QteBadFormat
                    | QteOther String
                    | QtePleaseSendBody
                    | QteBadNetworkMagic
                    | QteNetworkError String
                    | QteEncodingError String
                    | QteDispositionFailed
                    | QteLoggingError
                    | QteConnectionTimeout
                    | QteUnknownCommand 
                    | QteThrottle Int deriving (Show,Read,Typeable)

instance Binary TransmitStatus where
   put QteOK = putWord8 0
   put QteUnknownPid = putWord8 1
   put QteBadFormat = putWord8 2
   put (QteOther s) = putWord8 3 >> put s
   put QtePleaseSendBody = putWord8 4
   put QteBadNetworkMagic = putWord8 5
   put (QteNetworkError s) = putWord8 6 >> put s
   put (QteEncodingError s) = putWord8 7 >> put s
   put QteDispositionFailed = putWord8 8
   put QteLoggingError = putWord8 9
   put QteConnectionTimeout = putWord8 10
   put QteUnknownCommand = putWord8 11
   put (QteThrottle s) = putWord8 12
   get = do ch <- getWord8
            case ch of
              0 -> return QteOK
              1 -> return QteUnknownPid
              2 -> return QteBadFormat
              3 -> get >>= return . QteOther
              4 -> return QtePleaseSendBody
              5 -> return QteBadNetworkMagic
              6 -> get >>= return . QteNetworkError
              7 -> get >>= return . QteEncodingError
              8 -> return QteDispositionFailed
              9 -> return QteLoggingError
              10 -> return QteConnectionTimeout
              11 -> return QteUnknownCommand
              12 -> get >>= return . QteThrottle


----------------------------------------------
-- * Node and process spawning
----------------------------------------------

-- | Creates a new 'Node' object, given the specified configuration (usually created by 'readConfig') and
-- function metadata table (usually create by 'Remote.Call.registerCalls'). You probably want to use
-- 'Remote.Init.remoteInit' instead of this lower-level function.
initNode :: Config -> Lookup -> IO (MVar Node)
initNode cfg lookup = 
               do defaultHostName <- getHostName
                  let newHostName =
                       if null (cfgHostName cfg)
                          then defaultHostName
                          else cfgHostName cfg -- TODO it would be nice to check that is name actually makes sense, e.g. try to contact ourselves
                  theNewHostName <- evaluate newHostName
                  finalizer <- newEmptyMVar
                  finalized <- newEmptyMVar
                  outgoing <- newQSem (cfgMaxOutgoing cfg)
                  mvar <- newMVar Node {ndHostName=theNewHostName, 
                                        ndProcessTable=Map.empty,
                                        ndAdminProcessTable=Map.empty,
                                        ndConfig=cfg,
                                        ndLookup=lookup,
                                        ndListenPort=0,
                                        ndNextProcessId=0,
                                        ndNodeFinalizer=finalizer,
                                        ndNodeFinalized=finalized,
                                        ndLogConfig=defaultLogConfig,
                                        ndOutgoing=outgoing}
                  return mvar

-- | Given a Node (created by 'initNode'), start execution of user-provided code
-- by invoking the given function with the node's 'cfgRole' string.
roleDispatch :: MVar Node -> (String -> ProcessM ()) -> IO ()
roleDispatch mnode func = do cfg <- getConfigI mnode
                             runLocalProcess mnode (func (cfgRole cfg)) >> return ()

-- | Start executing a process on the current node. This is a variation of 'spawnLocal'
-- which accepts two blocks of user-defined code. The first block
-- is the main body of the code to run concurrently. The second block is a "prefix"
-- which is run in the new process, prior to the main body, but its completion
-- is guaranteed before spawnAnd returns. Thus, the prefix code is useful for
-- initializing the new process synchronously.
spawnLocalAnd :: ProcessM () -> ProcessM () -> ProcessM ProcessId
spawnLocalAnd fun prefix = 
                   do p <- getProcess
                      v <- liftIO $ newEmptyMVar
                      pid <- liftIO $ runLocalProcess (prNodeRef p) (myFun v)
                      liftIO $ takeMVar v
                      return pid
   where myFun mv = (prefix `Control.finally` liftIO (putMVar mv ())) >> fun

-- | A synonym for 'spawnLocal'
forkProcess :: ProcessM () -> ProcessM ProcessId
forkProcess = spawnLocal

-- | Create a parallel process sharing the same message queue and PID.
-- Not safe for export, as doing any message receive operation could
-- result in a munged message queue. 
forkProcessWeak :: ProcessM () -> ProcessM ()
forkProcessWeak f = do p <- getProcess
                       _res <- liftIO $ forkIO (runProcessM f p >> return ())
                       return ()

-- | Create a new process on the current node. Returns the new process's identifier.
-- Unlike 'spawn', this function does not need a 'Closure' or a 'NodeId'. 
spawnLocal :: ProcessM () -> ProcessM ProcessId
spawnLocal fun = do p <- getProcess
                    liftIO $ runLocalProcess (prNodeRef p) fun

runLocalProcess :: MVar Node -> ProcessM () -> IO ProcessId
runLocalProcess node fun = 
         do 
             localprocessid <- modifyMVar node (\thenode -> return $ (thenode {ndNextProcessId=1+ndNextProcessId thenode},
                                                                               ndNextProcessId thenode))
             channel <- newTChanIO
             passProcess <- newEmptyMVar
             okay <- newEmptyMVar
             thread <- forkIO (runner okay node passProcess)
             thenodeid <- getNodeId node
             let pid = thenodeid `seq` ProcessId thenodeid localprocessid
             state <- newTVarIO $ ProcessState {prQueue = queueMake}
             pool <- newIORef Map.empty
             putMVar passProcess (mkProcess channel node localprocessid thread pid state pool)
             takeMVar okay
             return pid
         where
          notifyProcessDown p r = do nid <- getNodeId node
                                     let pp = adminGetPid nid ServiceProcessMonitor
                                     let msg = GlProcessDown (prPid p) r
                                     try $ sendBasic node pp (msg) (Nothing::Maybe ()) PldAdmin Nothing :: IO (Either SomeException TransmitStatus)--ignore result ok
          notifyProcessUp _p = return ()
          closePool p = do c <- readIORef (prPool p)
                           mapM hClose (Map.elems c)
          exceptionHandler e p = let shown = show e in
                notifyProcessDown (p) (SrException shown) >>
                 (try (logI node  (prPid p) "SYS" LoCritical (concat ["Process got unhandled exception ",shown]))::IO(Either SomeException ())) >> return () --ignore error
          exceptionCatcher p fun = 
                 do notifyProcessUp (p)
                    res <- try (fun `catch` (\ProcessTerminationException -> return ()))
                    case res of
                      Left e -> exceptionHandler (e::SomeException) p
                      Right a -> notifyProcessDown (p) SrNormal >> closePool p >> return a
          runner okay node passProcess = do
                  p <- takeMVar passProcess
                  let init = do death <- newEmptyMVar
                                death2 <- newTVarIO False
                                let pte = (mkProcessTableEntry (prChannel p) (prThread p) death death2 )
                                insertProcessTableEntry node (prSelf p) pte
                                return pte
                  let action = runProcessM fun p
                  bracket (init)
                          (\pte -> atomically (writeTVar (pteDeathT pte) True) >> putMVar (pteDeath pte) ())
                          (\_ -> putMVar okay () >> 
                                   (exceptionCatcher p (action>>=return . snd)) >> return ())
          insertProcessTableEntry node processid entry =
               modifyMVar_ node (\n -> 
                    return $ n {ndProcessTable = Map.insert processid entry (ndProcessTable n)} )
          mkProcessTableEntry channel thread death death2 = 
             ProcessTableEntry
                {
                  pteChannel = channel,
                  pteThread = thread,
                  pteDeath = death,
                  pteDeathT = death2,
                  pteDaemonic = False
                }
          mkProcess channel noderef localprocessid thread pid state pool =
             Process
                {
                  prPid = pid,
                  prSelf = localprocessid,
                  prChannel = channel,
                  prNodeRef = noderef,
                  prThread = thread,
                  prState = state,
                  prPool = pool,
                  prLogConfig = Nothing
                }


----------------------------------------------
-- * Roundtrip conversations
----------------------------------------------

-- TODO this needs withMonitor a safe variant
-- To make this work with a ptimeout but still be able to return martial results,
-- they need to be stored in an MVar. Also, like roundtipQuery, this should have
-- two variants: a flavor that establishes monitors, and a timeout-based flavor.
-- This also needs an ASYNC variant, that will send data simultaneously, from multiple subprocesses
-- and finally, it needs a POLY variant, of type Pld -> [(ProcessId,a)] -> ProcessM [Either TransmitStatus b]
roundtripQueryMulti :: (Serializable a,Serializable b) => PayloadDisposition -> [ProcessId] -> a -> ProcessM [Either TransmitStatus b]
roundtripQueryMulti pld pids dat = -- TODO timeout
                      let 
                          convidsM = mapM (\_ -> liftIO $ newConversationId) pids
                       in do convids <- convidsM
                             sender <- getSelfPid
                             let
                                convopids = zip convids pids
                                sending (convid,pid) = 
                                   let hdr = RoundtripHeader {msgheaderConversationId = convid,
                                                              msgheaderSender = sender,
                                                              msgheaderDestination = pid}
                                    in do res <- sendTry pid dat (Just hdr) pld
                                          case res of
                                               QteOK -> return (convid,Nothing)
                                               n -> return (convid,Just (Left n))
                             res <- mapM sending convopids
                             let receiving c = if (any isNothing (Map.elems c)) 
                                                   then do newmap <- receiveWait [matcher c]
                                                           receiving newmap
                                                   else return c
                                 matcher c = matchCore (\(_,h) -> case h of
                                                                       Just a -> (msgheaderConversationId a,msgheaderSender a) `elem` convopids
                                                                       Nothing -> False)
                                                         (\(b,Just h) -> 
                                                               let newmap = Map.adjust (\_ -> (Just (Right b))) (msgheaderConversationId h) c
                                                                in return (newmap) )
                             m <- receiving (Map.fromList res)
                             return $ catMaybes (Map.elems m)

generalPid :: ProcessId -> ProcessId
generalPid (ProcessId n _p) = ProcessId n (-1)

roundtripQuery :: (Serializable a, Serializable b) => PayloadDisposition -> ProcessId -> a -> ProcessM (Either TransmitStatus b)
roundtripQuery pld pid dat =
    do res <- Control.try $ withMonitor apid $ roundtripQueryImpl 0 pld pid dat Prelude.id []
       case res of
         Left (ServiceException s) -> return $ Left $ QteOther s
         Right (Left a) -> return (Left a)
         Right (Right a) -> return (Right a)
  where apid = case pld of
                   PldAdmin -> generalPid pid
                   _ -> pid

roundtripQueryLocal :: (Serializable a, Serializable b) => PayloadDisposition -> ProcessId -> a -> ProcessM (Either TransmitStatus b)
roundtripQueryLocal pld pid dat = roundtripQueryImpl 0 pld pid dat Prelude.id []

roundtripQueryUnsafe :: (Serializable a, Serializable b) => PayloadDisposition -> ProcessId -> a -> ProcessM (Either TransmitStatus b)
roundtripQueryUnsafe pld pid dat = 
                       do cfg <- getConfig
                          roundtripQueryImpl (cfgRoundtripTimeout cfg) pld pid dat Prelude.id []

roundtripQueryImpl :: (Serializable a, Serializable b) => Int -> PayloadDisposition -> ProcessId -> a -> (b -> c) -> [MatchM (Either TransmitStatus c) ()] -> ProcessM (Either TransmitStatus c)
roundtripQueryImpl time pld pid dat converter additional =
    do mmatcher <- roundtripQueryImplSub pld pid dat (\_ y -> (return . Right . converter) y)
       case mmatcher of
            Left err -> return $ Left err
            Right matcher ->
                     receiver $ [ matcher (),
                                  matchProcessDown pid ((return . Left . QteNetworkError) "Remote partner unavailable"),
                                  matchProcessDown (generalPid pid) ((return . Left . QteNetworkError) "Remote partner unavailable")]
                                   ++ additional
   where 
         receiver matchers = 
                     case time of
                        0 -> do receiveWait matchers
                        n -> do res <- receiveTimeout n matchers
                                case res of
                                   Nothing -> return (Left QteConnectionTimeout)
                                   Just a -> return a

roundtripQueryImplSub :: (Serializable a, Serializable b) => PayloadDisposition -> ProcessId -> a -> (c -> b -> ProcessM q) -> ProcessM (Either TransmitStatus (c -> MatchM q ()))
roundtripQueryImplSub pld pid dat act =
    do convId <- liftIO $ newConversationId
       sender <- getSelfPid
       res <- mysend pid dat (Just RoundtripHeader {msgheaderConversationId = convId,msgheaderSender = sender,msgheaderDestination = pid})
       case res of
            QteOK -> return $ Right $ \c -> (matchCore (\(_,h) -> 
              case h of
                 Just a -> msgheaderConversationId a == convId && msgheaderSender a == pid
                 Nothing -> False) (\(x,_) -> do vv <- act c x
                                                 return $ vv))
            err -> return (Left err)
   where 
         mysend p d mh = sendTry p d mh pld

roundtripResponse :: (Serializable a, Serializable b) => (a -> ProcessM (b,q)) -> MatchM q ()
roundtripResponse f = roundtripResponseAsync myf False
     where myf inp verf = do (resp,ret) <- f inp
                             _ <- verf resp
                             return ret

roundtripResponseAsync :: (Serializable a, Serializable b) => (a -> (b -> ProcessM ()) -> ProcessM q) -> Bool -> MatchM q ()
roundtripResponseAsync f throwing =
       matchCore (\(_,h)->conditional h) transformer
   where conditional :: Maybe RoundtripHeader -> Bool
         conditional a = case a of
                           Just _ -> True
                           Nothing -> False
         transformer (m,Just h) = 
                            let sender b =
                                 do res <- sendTry (msgheaderSender h) b (Just RoundtripHeader {msgheaderSender=msgheaderDestination h,msgheaderDestination = msgheaderSender h,msgheaderConversationId=msgheaderConversationId h}) PldUser
                                    case res of
                                      QteOK -> return ()
                                      _ -> if throwing
                                              then throw $ TransmitException res
                                              else logS "SYS" LoImportant ("roundtripResponse couldn't send response to "++show (msgheaderSender h)++" because "++show res)

                             in f m sender

data RoundtripHeader = RoundtripHeader
    {
       msgheaderConversationId :: Int,
       msgheaderSender :: ProcessId,
       msgheaderDestination :: ProcessId
    } deriving (Typeable)
instance Binary RoundtripHeader where
   put (RoundtripHeader a b c) = put a >> put b >> put c
   get = get >>= \a -> get >>= \b -> get >>= \c -> return $ RoundtripHeader a b c
                                 
----------------------------------------------
-- * Message sending
----------------------------------------------

-- | Sends a message to the given process. If the
-- process isn't running or can't be accessed,
-- this function will throw a 'TransmitException'.
-- The message must implement the 'Serializable' interface.
send :: (Serializable a) => ProcessId -> a -> ProcessM ()
send pid msg = sendSimple pid msg PldUser >>= 
                                           (\x -> case x of
                                                    QteOK -> return ()
                                                    _ -> throw $ TransmitException x
                                          )

-- | Like 'send', but in case of error returns a value rather than throw
-- an exception.
sendQuiet :: (Serializable a) => ProcessId -> a -> ProcessM TransmitStatus
sendQuiet p m = sendSimple p m PldUser

sendSimple :: (Serializable a) => ProcessId -> a -> PayloadDisposition -> ProcessM TransmitStatus
sendSimple pid dat pld = sendTry pid dat (Nothing :: Maybe ()) pld

sendTry :: (Serializable a,Serializable b) => ProcessId -> a -> Maybe b -> PayloadDisposition -> ProcessM TransmitStatus
sendTry pid msg msghdr pld = getProcess >>= (\_p -> 
       let
          timeoutFilter a =
             do cfg <- getConfig
-- TODO This is problematic. We should give up on transmitting after a certain period
-- of time if the remote host is just not responsive, but delays in message delivery,
-- even to legitimate peers, are unpredictable. Timeouts are currently disabled
-- but should be enabled if I can think of a good way to set them. This would
-- also be a good place to put automatic retries or to limit the number
-- of outgoing connections per node. //!!
                q <- case 0 of -- case cfgRoundtripTimeout cfg of 
                   0 -> a
                   n -> do ff <- ptimeout n $ a
                           case ff of
                              Nothing -> return QteConnectionTimeout
                              Just r -> return r
                case q of
                   QteThrottle n -> liftIO (threadDelay n) >> timeoutFilter a
                   n -> return n
          tryAction = Control.try action >>=
                    (\x -> case x of
                              Left l -> return $ QteEncodingError $ show (l::ErrorCall) -- catch errors from encoding
                              Right r -> return r)
               where action = 
                      do p <- getProcess
                         timeoutFilter $ liftIO $ sendBasic (prNodeRef p) pid msg msghdr pld (Just $ prPool p)

       in tryAction )      

sendBasic :: (Serializable a,Serializable b) => MVar Node -> ProcessId -> a -> Maybe b -> PayloadDisposition -> Maybe (IORef (Map.Map NodeId Handle)) -> IO TransmitStatus
sendBasic mnode pid msg msghdr pld pool = do
              nid <- getNodeId mnode
              let islocal = nodeFromPid pid == nid

              -- TODO It's important that the semantics of messaging are preserved
              -- regardles of the location of particular processes. Messages are
              -- fully evaluated when sent to a remote node as part of the serialization
              -- but they don't need to be when sending to a local process; the message
              -- is just plopped into a TChan, basically. Unfortunately, this means
              -- that exceptions associated with the serialization of data will be
              -- handled by the receiving process, rather than the sender, which
              -- it may not be prepared for. An easy way to crash a system process,
              -- then, is to send it a message contained undefined or divide by zero.
              -- To prevent this, we now serialize all messages, even those bound for
              -- local destinations.
              -- FORMERLY: let themsg = makeMessage islocal pld msg msghdr
              let themsg = makeMessage False pld msg msghdr
              (if islocal then sendRawLocal else sendRawRemote) mnode pid nid themsg pool

sendRawLocal :: MVar Node -> ProcessId -> NodeId -> Message -> Maybe (IORef (Map.Map NodeId Handle)) -> IO TransmitStatus
sendRawLocal noderef thepid _nodeid msg _
     | thepid == nullPid = return QteUnknownPid
     | otherwise = do cfg <- getConfigI noderef
                      messageHandler cfg noderef (getMessageDisposition msg) msg (cfgNetworkMagic cfg) (localFromPid thepid)

sendRawRemote :: MVar Node -> ProcessId -> NodeId -> Message -> Maybe (IORef (Map.Map NodeId Handle)) -> IO TransmitStatus
sendRawRemote noderef thepid nodeid msg (Just pool) =
    do apool <- readIORef pool
       ppool <- if Map.size apool > 10 -- trim pool
                   then mapM hClose (Map.elems apool) >> return Map.empty
                   else return apool
       let finded = (Map.lookup thenode ppool) 
       (ret,h) <- sendRawRemoteImpl noderef thepid nodeid msg finded
       case ret of
         QteOK -> cleanup h ppool
         _ -> case finded of
                Nothing -> cleanup h ppool
                _ -> do (_ret2,newh) <- sendRawRemoteImpl noderef thepid nodeid msg Nothing
                        cleanup newh ppool
       return ret
  where
      thenode = (nodeFromPid thepid)
      cleanup h ppool =        
       case h of
         Just hh -> writeIORef pool (Map.insert thenode hh ppool)
         Nothing -> writeIORef pool (Map.delete thenode ppool)
sendRawRemote noderef thepid nodeid msg Nothing = 
        liftM fst $ sendRawRemoteImpl noderef thepid nodeid msg Nothing

                  
sendRawRemoteImpl :: MVar Node -> ProcessId -> NodeId -> Message -> Maybe Handle -> IO (TransmitStatus,Maybe Handle)
sendRawRemoteImpl noderef thepid@(ProcessId (NodeId hostname portid) localpid) nodeid msg bigh
     | thepid == nullPid = return (QteUnknownPid,Nothing)
     | otherwise = 
         do node <- readMVar noderef
            res <- withSem (ndOutgoing node) (try setup)
            case res of
               Right n -> return n
               Left l | isEOFError l -> return (QteNetworkError (show l),Nothing)
                      | isUserError l -> return (QteBadFormat,Nothing)
                      | isDoesNotExistError l -> return (QteNetworkError (show l),Nothing)
                      | otherwise -> return (QteOther $ show l,Nothing)
    where setup = bracket 
                 (acquireConnection)
                 (\_ -> return ())
                 (sender)
          acquireConnection = 
               case bigh of
                   Nothing ->
                      do h <- connectTo hostname (PortNumber $ toEnum portid)
                         hSetBuffering h (BlockBuffering Nothing)
                         return h
                   Just h -> return h
          sender h = do cfg <- getConfigI noderef
                        ret <- writeMessage h (cfgNetworkMagic cfg,localpid,nodeid,msg)
                        return (ret,Just h)
    
writeMessage :: Handle -> (String,LocalProcessId,NodeId,Message)-> IO TransmitStatus
writeMessage h (magic,dest,nodeid,(EncodedMessage msgDisp msgHdr msgMsg)) = 
         do hPutStrZ h $ unwords ["Rmt!!",magic,show dest,show msgDisp,show fmt,show nodeid]
            hFlush h
            response <- hGetLineZ h
            resp <- readIO response :: IO TransmitStatus
            case resp of
               QtePleaseSendBody -> do maybe (return ()) (hPutPayload h) (msgHdr)
                                       hPutPayload h msgMsg
                                       hFlush h
                                       response2 <- hGetLineZ h
                                       resp2 <- readIO response2 :: IO TransmitStatus
                                       return resp2
               QteOK -> return QteBadFormat
               n -> return n                
         where fmt = case msgHdr of
                        Nothing -> (0::Int)
                        Just _ -> 2
writeMessage _ _ = throw $ ServiceException "writeMessage went down wrong pipe"

----------------------------------------------
-- * Message delivery
----------------------------------------------

-- | Starts a message-receive loop on the given node. You probably don't want to call this function yourself.
forkAndListenAndDeliver :: MVar Node -> Config -> IO ()
forkAndListenAndDeliver node cfg = do coord <- newEmptyMVar
                                      _tid <- forkIO $ listenAndDeliver node cfg (coord)
                                      result <- takeMVar coord
                                      maybe (return ()) throw result

writeResult :: Handle -> TransmitStatus -> IO ()
writeResult h er = hPutStrZ h (show er) >> hFlush h

readMessage :: Handle -> IO (String,LocalProcessId,NodeId,Message)
readMessage h = 
              do line <- hGetLineZ h
                 case words line of
                  ["Rmt!!",magic,destp,disp,format,nodeid] ->
                     do adestp <- readIO destp :: IO LocalProcessId 
                        adisp <- readIO disp :: IO PayloadDisposition
                        aformat <- readIO format :: IO Int
                        anodeid <- readIO nodeid :: IO NodeId

                        writeResult h QtePleaseSendBody
                        hFlush h
                        header <- case aformat of
                                     2 -> do hdr <- hGetPayload h
                                             return $ Just hdr
                                     _ -> return Nothing
                        body <- hGetPayload h
                        return (magic,adestp,anodeid,EncodedMessage { msgEHeader = header,
                                           msgEDisposition = adisp,
                                           msgEPayload = body
                                           })
                  _ -> throw $ userError "Bad message format"

getProcessTableEntry :: Node -> LocalProcessId -> STM (Maybe ProcessTableEntry)
getProcessTableEntry tbl adestp = let res = Map.lookup adestp (ndProcessTable tbl) in
                                  case res of
                                     Just x -> do isdead <- readTVar (pteDeathT x)
                                                  if isdead
                                                     then return Nothing
                                                     else return (Just x)
                                     Nothing -> return Nothing

deliver :: LocalProcessId -> MVar Node -> Message -> IO (Maybe ProcessTableEntry)
deliver adestp tbl msg = 
         do
            node <- readMVar tbl
            atomically $ do
                            pte <- getProcessTableEntry node adestp 

                            maybe (return Nothing) (\x -> writeTChan (pteChannel x) msg >> return (Just x)) pte
                               
listenAndDeliver :: MVar Node -> Config -> MVar (Maybe IOError) -> IO ()
listenAndDeliver node cfg coord = 
-- this listenon should be replaced with a lower-level listen call that binds
-- to the interface corresponding to the name specified in cfgNodeName
  do res <- try $ bracket (setupSocket) (sClose) (\s -> finishStartup >> sockBody s)
     case res of
       Left e -> putMVar coord (Just e)
       Right _ -> return ()
   where  
         finishStartup = putMVar coord Nothing
         setupSocket =            
               do sock <- listenOn whichPort
                  setSocketOption sock KeepAlive 1
                  realPort <- socketPort sock
                  modifyMVar_ node (\a -> return $ a {ndListenPort=fromEnum realPort}) 
                  return sock
         whichPort = if cfgListenPort cfg /= 0
                        then PortNumber $ toEnum $ cfgListenPort cfg
                        else PortNumber aNY_PORT
         handleCommSafe h = 
            (try $ handleComm h :: IO (Either IOError ())) >> return ()
{- UNUSED
         logNetworkError :: IOError -> IO ()
         logNetworkError n = return ()
         writeResultTry h q =
            do res <- try (writeResult h q)
               case res of
                  Left n -> logNetworkError n
                  Right q -> return ()
-}
         handleComm h = 
            do (magic,adestp,_nodeid,msg) <- readMessage h
               res <- messageHandler cfg node (getMessageDisposition msg) msg magic adestp
               writeResult h res
               case res of
                 QteOK -> handleComm h
                 _ -> return ()
         sockBody s =
              do hchan <- newChan
                 _tid <- forkIO $ forever $ do h <- readChan hchan
                                               hSetBuffering h (BlockBuffering Nothing)
                                               forkIO $ (handleCommSafe h `finally` hClose h)
                 forever $ do (newh,_,_) <- accept s
                              writeChan hchan newh


messageHandler :: Config -> MVar Node -> PayloadDisposition -> Message -> String -> LocalProcessId -> IO TransmitStatus
messageHandler cfg node pld = case pld of 
                       PldAdmin -> dispositionAdminHandler
                       PldUser -> dispositionUserHandler
   where
         validMagic magic = magic == cfgNetworkMagic cfg         -- same network
                         || magic == localRegistryMagicMagic     -- message from local magic-neutral registry
                         || cfgRole cfg == localRegistryMagicRole-- message to local magic-neutral registry
         dispositionAdminHandler msg magic adestp 
            | validMagic magic = 
                 do 
                    realPid <- adminLookupN (toEnum adestp) node
                    case realPid of
                         Left er -> return er
                         Right p -> do res <- deliver p node msg
                                       case res of
                                         Just _ -> return QteOK
                                         Nothing -> return QteUnknownPid
            | otherwise = return QteBadNetworkMagic

         dispositionUserHandler msg magic adestp
            | validMagic magic = 
                     do 
                        res <- deliver adestp node msg
                        case res of
                           Nothing -> return QteUnknownPid
                           Just _ -> return QteOK
            | otherwise = return QteBadNetworkMagic

-- | Blocks until all non-daemonic processes of the given
-- node have ended. Usually called on the main thread of a program.
waitForThreads :: MVar Node -> IO ()
waitForThreads mnode = do node <- takeMVar mnode
                          waitFor (Map.toList (ndProcessTable node)) node
  where waitFor lst node= case lst of
                             [] -> putMVar mnode node
                             (pn,pte):rest -> if (pteDaemonic pte)
                                                 then waitFor rest node
                                                 else do putMVar mnode node 
                                                         -- this take and modify should be atomic to ensure no one squeezes in a message to a zombie process
                                                         takeMVar  (pteDeath pte)
                                                         modifyMVar_ mnode (\x -> return $ x {ndProcessTable=Map.delete pn (ndProcessTable x)})
                                                         waitForThreads mnode

----------------------------------------------
-- * Miscellaneous utilities
----------------------------------------------

paddedString :: Int -> String -> String
paddedString i s = s ++ (replicate (i-length s) ' ')

newConversationId :: IO Int
newConversationId = do d <- newUnique
                       return $ hashUnique d

-- TODO this is a huge performance bottleneck. Why? How to fix?
exclusionList :: [a] -> [(a,[a])]
exclusionList [] = []
exclusionList (a:rest) = each [] a rest
  where
      each before it [] = [(it,before)]
      each before it following@(next:after) = (it,before++following):(each (before++[it]) next after)

{-
dumpMessageQueue :: ProcessM [String]
dumpMessageQueue = do p <- getProcess
                      msgs <- cleanChannel
                      ps <- liftIO $ modifyIORef (prState p) (\x -> x {prQueue = queueInsertMulti  (prQueue x) msgs})
                      buf <- liftIO $ readIORef (prState p)
                      return $ map msgPayload (queueToList $ prQueue buf)
                      

printDumpMessageQueue :: ProcessM ()
printDumpMessageQueue = do liftIO $ putStrLn "----BEGINDUMP------"
                           dump <- dumpMessageQueue
                           liftIO $ mapM putStrLn dump
                           liftIO $ putStrLn "----ENDNDUMP-------"
-}

{- UNUSED
duration :: Int -> ProcessM a -> ProcessM (Int,a)
duration t a = 
           do time1 <- liftIO $ getCurrentTime
              result <- a
              time2 <- liftIO $ getCurrentTime
              return (t - diffTime time2 time1,result)
-}

diffTime :: UTCTime -> UTCTime -> Int
diffTime time2 time1 =
   picosecondsToMicroseconds (fromEnum (diffUTCTime time2 time1))
   where picosecondsToMicroseconds a = a `div` 1000000


hGetLineZ :: Handle -> IO String
hGetLineZ h = loop [] >>= return . reverse
      where loop l = do c <- hGetChar h
                        case c of
                             '\0' -> return l
                             _ -> loop (c:l)

hPutStrZ :: Handle -> String -> IO ()
hPutStrZ h [] = hPutChar h '\0'
hPutStrZ h (c:l) = hPutChar h c >> hPutStrZ h l

buildPid :: ProcessM ()
buildPid = do  p <- getProcess
               node <- liftIO $ readMVar (prNodeRef p)
               let pid=ProcessId (NodeId (ndHostName node) (ndListenPort node))
                                 (prSelf p)
               putProcess $ p {prPid = pid}

nullPid :: ProcessId
nullPid = ProcessId (NodeId "0.0.0.0" 0) 0

-- | Returns the node ID of the node that the current process is running on.
getSelfNode :: ProcessM NodeId
getSelfNode = do (ProcessId n _p) <- getSelfPid
                 return n

getNodeId :: MVar Node -> IO NodeId
getNodeId mnode = do node <- readMVar mnode
                     return (NodeId (ndHostName node) (ndListenPort node))

-- | Returns the process ID of the current process.
getSelfPid :: ProcessM ProcessId
getSelfPid = do p <- getProcess
                case prPid p of
                  (ProcessId (NodeId _ 0) _) -> (buildPid >> getProcess >>= return.prPid)
                  _ -> return $ prPid p

nodeFromPid :: ProcessId -> NodeId
nodeFromPid (ProcessId nid _) = nid

makeNodeFromHost :: String -> PortId -> NodeId
makeNodeFromHost = NodeId

localFromPid :: ProcessId -> LocalProcessId
localFromPid (ProcessId _ lid) = lid

hostFromNid :: NodeId -> HostName
hostFromNid (NodeId hn _p) = hn

buildPidFromNodeId :: NodeId -> LocalProcessId -> ProcessId
buildPidFromNodeId n lp = ProcessId n lp

{- UNUSED
localServiceToPid :: LocalProcessId -> ProcessM ProcessId
localServiceToPid sid = do (ProcessId nid _lid) <- getSelfPid
                           return $ ProcessId nid sid
-}

-- | Returns true if the given process ID is associated with the current node.
-- Does not examine if the process is currently running.
isPidLocal :: ProcessId -> ProcessM Bool
isPidLocal pid = do mine <- getSelfPid
                    return (nodeFromPid mine == nodeFromPid pid)


----------------------------------------------
-- * Exception handling
----------------------------------------------

suppressTransmitException :: ProcessM a -> ProcessM (Maybe a)
suppressTransmitException a = 
     do res <- Control.try a
        case res of
          Left (TransmitException _) -> return Nothing
          Right r -> return $ Just r

-- | A 'ProcessM'-flavoured variant of 'System.Timeout.timeout'
ptimeout :: Int -> ProcessM a -> ProcessM (Maybe a)
ptimeout t f = do p <- getProcess
                  res <- liftIO $ System.Timeout.timeout t (runProcessM f p)
                  case res of
                    Nothing -> return Nothing
                    Just (newp,newanswer) -> ProcessM (\_ -> return (newp,Just newanswer))

----------------------------------------------
-- * Configuration file
----------------------------------------------
  
emptyConfig :: Config
emptyConfig = Config {
                  cfgRole = "NODE",
                  cfgHostName = "",
                  cfgListenPort = 0,
                  cfgPeerDiscoveryPort = 38813,
                  cfgLocalRegistryListenPort = 38813,
                  cfgNetworkMagic = "MAGIC",
                  cfgKnownHosts = [],
                  cfgRoundtripTimeout = 10000000,
                  cfgMaxOutgoing = 50,
                  cfgPromiseFlushDelay = 5000000,
                  cfgPromisePrefix = "rpromise-",
                  cfgArgs = []
                  }

-- | Reads in configuration data from external sources, specifically from the command line arguments
-- and a configuration file. 
-- The first parameter to this function determines whether command-line arguments are consulted.
-- If the second parameter is not 'Nothing' then it should be the name of the configuration file;
-- an exception will be thrown if the specified file does not exist.
-- Usually, this function shouldn't be called directly, but rather from 'Remote.Init.remoteInit',
-- which also takes into account environment variables.
-- Options set by command-line parameters have the highest precedence,
-- followed by options read from a configuration file; if a configuration option is not explicitly
-- specified anywhere, a reasonable default is used. The configuration file has a format, wherein
-- one configuration option is specified on each line; the first token on each line is the name
-- of the configuration option, followed by whitespace, followed by its value. Lines beginning with #
-- are comments. Thus:
--
-- > # This is a sample configuration file
-- > cfgHostName host3
-- > cfgKnownHosts host1 host2 host3 host4
--
-- Options may be specified on the command line similarly. Note that command-line arguments containing spaces must be quoted.
--
-- > ./MyProgram -cfgHostName=host3 -cfgKnownHosts='host1 host2 host3 host4'
readConfig :: Bool -> Maybe FilePath -> IO Config
readConfig useargs fp = do a <- safety (processConfigFile emptyConfig) ("while reading config file ")
                           b <- safety (processArgs a) "while parsing command line "
                           return b
     where  processConfigFile from = maybe (return from) (readConfigFile from) fp
            processArgs from = if useargs
                                  then readConfigArgs from
                                  else return from
            safety o s = do res <- try o :: IO (Either SomeException Config)
                            either (\e -> throw $ ConfigException $ s ++ show e) (return) res
            readConfigFile from afile = do contents <- readFile afile
                                           evaluate $ processConfig (lines contents) from
            readConfigArgs from = do args <- getArgs
                                     c <- evaluate $ processConfig (fst $ collectArgs args) from
                                     return $ c {cfgArgs = reverse $ snd $ collectArgs args}
            collectArgs the = foldl findArg ([],[]) the
            findArg (last,n) ('-':this) | isPrefixOf "cfg" this = ((map (\x -> if x=='=' then ' ' else x) this):last,n)
            findArg (last,n) a          = (last,a:n)

processConfig :: [String] -> Config -> Config
processConfig rawLines from = foldl processLine from rawLines
 where
  processLine cfg line = case words line of
                            ('#':_):_ -> cfg
                            [] -> cfg
                            [option,val] -> updateCfg cfg option val
                            option:rest | (not . null) rest -> updateCfg cfg option (unwords rest)
                            _ -> error $ "Bad configuration syntax: " ++ line
  updateCfg cfg "cfgRole" role = cfg {cfgRole=clean role}
  updateCfg cfg "cfgHostName" hn = cfg {cfgHostName=clean hn}
  updateCfg cfg "cfgListenPort" p = cfg {cfgListenPort=(read.isInt) p}
  updateCfg cfg "cfgPeerDiscoveryPort" p = cfg {cfgPeerDiscoveryPort=(read.isInt) p}
  updateCfg cfg "cfgRoundtripTimeout" p = cfg {cfgRoundtripTimeout=(read.isInt) p}
  updateCfg cfg "cfgPromiseFlushDelay" p = cfg {cfgPromiseFlushDelay=(read.isInt) p}
  updateCfg cfg "cfgMaxOutgoing" p = cfg {cfgMaxOutgoing=(read.isInt) p}
  updateCfg cfg "cfgPromisePrefix" p = cfg {cfgPromisePrefix=p}
  updateCfg cfg "cfgLocalRegistryListenPort" p = cfg {cfgLocalRegistryListenPort=(read.isInt) p}
  updateCfg cfg "cfgKnownHosts" m = cfg {cfgKnownHosts=words m}
  updateCfg cfg "cfgNetworkMagic" m = cfg {cfgNetworkMagic=clean m}
  updateCfg _   opt _ = error ("Unknown configuration option: "++opt)
  isInt s | all isDigit s = s
  isInt s = error ("Not a good number: "++s)
{- UNUSED
  nonempty s | (not.null) s = s
  nonempty b = error ("Unexpected empty item: " ++ b)
-}
  clean = filter (not.isSpace)

----------------------------------------------
-- * Logging
----------------------------------------------

-- | Specifies the importance of a particular log entry.
-- Can also be used to filter log output.
data LogLevel = LoSay -- ^ Non-suppressible application-level emission
              | LoFatal
              | LoCritical 
              | LoImportant
              | LoStandard -- ^ The default log level
              | LoInformation
              | LoTrivial 
                deriving (Eq,Ord,Enum,Show)

instance Binary LogLevel where
   put n = put $ fromEnum n
   get = get >>= return . toEnum

-- | Specifies the subsystem or region that is responsible for
-- generating a given log entry. This is useful in conjunction
-- with 'LogFilter' to limit displayed log output to the
-- particular area of your program that you are currently debugging.
-- The SYS, TSK, and SAY spheres are used by the framework
-- for messages relating to the Process layer, the Task layer,
-- and the 'say' function.
-- The remainder of values are free for use at the application level.
type LogSphere = String

-- | A preference as to what is done with log messages
data LogTarget = LtStdout -- ^ Messages will be output to the console; the default
               | LtForward NodeId  -- ^ Log messages will be forwarded to the given node; please don't set up a loop
               | LtFile FilePath -- ^ Log messages will be appended to the given file
               | LtForwarded  -- ^ Special value -- don't set this in your LogConfig!
               deriving (Typeable)

instance Binary LogTarget where
     put LtStdout = putWord8 0
     put (LtForward nid) = putWord8 1 >> put nid
     put (LtFile fp) = putWord8 2 >> put fp
     put LtForwarded = putWord8 3
     get = do a <- getWord8
              case a of
                0 -> return LtStdout
                1 -> get >>= return . LtForward
                2 -> get >>= return . LtFile
                3 -> return LtForwarded

-- | Specifies which log messages will be output. 
-- All log messages of importance below the current
-- log level or not among the criterea given here
-- will be suppressed. This type lets you limit
-- displayed log messages to certain components.
data LogFilter = LfAll
               | LfOnly [LogSphere]
               | LfExclude [LogSphere] deriving (Typeable)

instance Binary LogFilter where
      put LfAll = putWord8 0
      put (LfOnly ls) = putWord8 1 >> put ls
      put (LfExclude le) = putWord8 2 >> put le
      get = do a <- getWord8
               case a of
                  0 -> return LfAll
                  1 -> get >>= return . LfOnly
                  2 -> get >>= return . LfExclude

-- | Expresses a current configuration of the logging
-- subsystem, which determines which log messages to 
-- be output and where to send them when they are.
-- Both processes and nodes have log configurations,
-- set with 'setLogConfig' and 'setNodeLogConfig'
-- respectively. The node log configuration is
-- used for all processes that have not explicitly
-- set their log configuration. Otherwise, the
-- process log configuration takes priority.
data LogConfig = LogConfig
     {
       logLevel :: LogLevel, -- ^ The lowest message priority that will be displayed
       logTarget :: LogTarget, -- ^ Where to send messages
       logFilter :: LogFilter -- ^ Other filtering
     } deriving (Typeable)

instance Binary LogConfig where
   put (LogConfig ll lt lf) = put ll >> put lt >> put lf
   get = do ll <- get
            lt <- get
            lf <- get
            return $ LogConfig ll lt lf

data LogMessage = LogMessage UTCTime LogLevel LogSphere String ProcessId LogTarget
                | LogUpdateConfig LogConfig deriving (Typeable)

-- This correct instance of UTCTime
-- works on 32-bit systems and is
-- courtesy of Warren Harris
instance Binary UTCTime where
  put t = do
    let d = toModifiedJulianDay (utctDay t)
        s = realToFrac $ utctDayTime t :: Pico
        ps = truncate $ s * 1e12 :: Integer
    put d >> put ps
  get = do
    d <- get
    ps <- get
    return $ UTCTime (ModifiedJulianDay d) (picosecondsToDiffTime ps)

instance Binary LogMessage where
  put (LogMessage utc ll ls s pid target) = putWord8 0 >> put utc >> put ll >> put ls >> put s >> put pid >> put target
  put (LogUpdateConfig lc) = putWord8 1 >> put lc
  get = do a <- getWord8
           case a of
              0 -> do utc <- get
                      ll <- get
                      ls <- get
                      s <- get
                      pid <- get
                      target <- get
                      return $ LogMessage utc ll ls s pid target
              1 -> do lc <- get
                      return $ LogUpdateConfig lc

instance Show LogMessage where
  show (LogMessage utc ll ls s pid _) = concat [paddedString 30 (show utc)," ",(show $ fromEnum ll)," ",paddedString 28 (show pid)," ",ls," ",s]
  show (LogUpdateConfig _) = "LogUpdateConfig"

showLogMessage :: LogMessage -> IO String
showLogMessage (LogMessage utc ll ls s pid _) = 
   do gmt <- utcToLocalZonedTime utc
      return $ concat [paddedString 30 (show gmt)," ",
                       (show $ fromEnum ll)," ",
                       paddedString 28 (show pid)," ",ls," ",s]

-- | The default log configuration represents
-- a starting point for setting your own
-- configuration. It is:
--
-- > logLevel = LoStandard
-- > logTarget = LtStdout
-- > logFilter = LfAll
defaultLogConfig :: LogConfig
defaultLogConfig = LogConfig
                   {
                      logLevel = LoStandard,
                      logTarget = LtStdout,
                      logFilter = LfAll
                   }

logApplyFilter :: LogConfig -> LogSphere -> LogLevel -> Bool
logApplyFilter cfg sph lev = filterSphere (logFilter cfg) 
                          && filterLevel (logLevel cfg)
    where filterSphere LfAll = True
          filterSphere (LfOnly lst) = elem sph lst
          filterSphere (LfExclude lst) = not $ elem sph lst
          filterLevel ll = lev <= ll

-- | Uses the logging facility to produce non-filterable, programmatic output. Shouldn't be used
-- for informational logging, but rather for application-level output.
say :: String -> ProcessM ()
say v = v `seq` logS "SAY" LoSay v

-- | Gets the currently active log configuration
-- for the current process; if the current process
-- doesn't have a log configuration set, the process's
-- log configuration will be returned
getLogConfig :: ProcessM LogConfig
getLogConfig = do p <- getProcess
                  case (prLogConfig p) of
                    Just lc -> return lc
                    Nothing -> do node <- liftIO $ readMVar (prNodeRef p)
                                  return (ndLogConfig node)

-- | Set the process's log configuration. This overrides
-- any node-level log configuration
setLogConfig :: LogConfig -> ProcessM ()
setLogConfig lc = do p <- getProcess
                     putProcess (p {prLogConfig = Just lc})

-- | Sets the node's log configuration
setNodeLogConfig :: LogConfig -> ProcessM ()
setNodeLogConfig lc = do p <- getProcess
                         liftIO $ modifyMVar_ (prNodeRef p) (\x -> return $ x {ndLogConfig = lc})

-- | Sets the log configuration of a remote node.
-- May throw TransmitException
setRemoteNodeLogConfig :: NodeId -> LogConfig -> ProcessM ()
setRemoteNodeLogConfig nid lc = do res <- sendSimple (adminGetPid nid ServiceLog) (LogUpdateConfig lc) PldAdmin
                                   case res of 
                                     QteOK -> return ()
                                     _n -> throw $ TransmitException $ QteLoggingError

logI :: MVar Node -> ProcessId -> LogSphere -> LogLevel -> String -> IO ()
logI mnode pid sph ll txt = do node <- readMVar mnode
                               when (filtered (ndLogConfig node))
                                 (sendMsg (ndLogConfig node))
         where filtered cfg = logApplyFilter cfg sph ll
               makeMsg cfg = do time <- getCurrentTime
                                return $ LogMessage time ll sph txt pid (logTarget cfg)
               sendMsg cfg = do msg <- makeMsg cfg
                                res <- let svc = (adminGetPid nid ServiceLog)
                                           nid = nodeFromPid pid 
                                       in sendBasic mnode svc msg (Nothing::Maybe()) PldAdmin Nothing
                                case res of
                                    _ -> return () -- ignore error -- what can I do?
                                
                                  
-- | Generates a log entry, using the process's current logging configuration.
--
-- * 'LogSphere' indicates the subsystem generating this message. SYS in the case of componentes of the framework.
--
-- * 'LogLevel' indicates the importance of the message.
--
-- * The third parameter is the log message.
--
-- Both of the first two parameters may be used to filter log output.
logS :: LogSphere -> LogLevel -> String -> ProcessM ()
logS sph ll txt = do lc <- txt `seq` getLogConfig
                     when (filtered lc)
                         (sendMsg lc)
         where filtered cfg = logApplyFilter cfg sph ll
               makeMsg cfg = do time <- liftIO $ getCurrentTime
                                pid <- getSelfPid
                                return $ LogMessage time ll sph txt pid (logTarget cfg)
               sendMsg cfg = 
                 do msg <- makeMsg cfg
                    nid <- getSelfNode
                    res <- let svc = (adminGetPid nid ServiceLog)
                               in sendSimple svc msg PldAdmin
                    case res of
                      QteOK -> return ()
                      _n -> throw $ TransmitException $ QteLoggingError

startLoggingService :: ProcessM ()
startLoggingService = serviceThread ServiceLog logger
   where logger = receiveWait [matchLogMessage,matchUnknownThrow] >> logger
         matchLogMessage = match (\msg ->
           do mylc <- getLogConfig
              case msg of
                (LogMessage _ _ _ _ _ LtForwarded) -> processMessage (logTarget mylc) msg
                (LogMessage _ _ _ _ _ _) -> processMessage (targetPreference msg) msg
                (LogUpdateConfig lc) -> setNodeLogConfig lc)
         targetPreference (LogMessage _ _ _ _ _ a) = a
         forwardify (LogMessage a b c d e _) = LogMessage a b c d e LtForwarded
         processMessage whereto txt =
          do smsg <- liftIO $ showLogMessage txt
             case whereto of
              LtStdout -> liftIO $ putStrLn smsg
              LtFile fp -> (Control.try (liftIO (appendFile fp (smsg ++ "\n"))) :: ProcessM (Either IOError ()) ) >> return () -- ignore error - what can we do?
              LtForward nid -> do self <- getSelfNode
                                  when (self /= nid) 
                                    (sendSimple (adminGetPid nid ServiceLog) (forwardify txt) PldAdmin >> return ()) -- ignore error -- what can we do?
              _n -> throw $ ConfigException $ "Invalid message forwarded setting"


----------------------------------------------
-- * Node monitoring
----------------------------------------------

data NodeMonitorCommand = 
           NodeMonitorStart NodeId
         | NodeMonitorPing
           deriving (Typeable)
instance Binary NodeMonitorCommand where 
  put (NodeMonitorStart nid) = putWord8 0 >> put nid
  put (NodeMonitorPing) = putWord8 1
  get = do a <- getWord8
           case a of
              0 -> do b <- get
                      return $ NodeMonitorStart b
              1 -> return NodeMonitorPing

data NodeMonitorSignal = 
         NodeMonitorNodeFailure NodeId deriving (Typeable)
instance Binary NodeMonitorSignal where 
   put (NodeMonitorNodeFailure nid) = put nid
   get = do a <- get
            return $ NodeMonitorNodeFailure a

data NodeMonitorInformation = 
           NodeMonitorNodeDown NodeId
           deriving (Typeable)
instance Binary NodeMonitorInformation where 
   put (NodeMonitorNodeDown nid) = put nid
   get = do a <- get
            return $ NodeMonitorNodeDown a

monitorNode :: NodeId -> ProcessM ()
monitorNode nid = do 
                     mynid <- getSelfNode
                     res <- roundtripQueryLocal PldAdmin (adminGetPid mynid ServiceNodeMonitor) (NodeMonitorStart nid)
                     case res of
                       Right () -> return ()
                       _ -> (throw $ ServiceException $ "while contacting node monitor "++show res)

-- | Sends a small message to the specified node to determine if it's alive.
-- If the node cannot be reached or does not respond within a time frame, the function
-- will return False.
pingNode :: NodeId -> ProcessM Bool
pingNode nid = do res <- roundtripQueryUnsafe PldAdmin (adminGetPid nid ServiceNodeMonitor) (NodeMonitorPing)   
                  case res of
                       Right () -> return True
                       _ -> return False

-- TODO this can be re-engineered to avoid setting up and tearing down TCP connections and threads
-- at every ping. Instead open up a TCP connection to the pingee and require it to send us a hello
-- every N seconds or die. Also, this has a bug in that the number of failures is not reset after a successful ping
startNodeMonitorService :: ProcessM ()
startNodeMonitorService = serviceThread ServiceNodeMonitor (service Map.empty)
  where
    service state = 
      let matchCommand cmd = case cmd of
                               NodeMonitorPing -> return ((),state)
                               NodeMonitorStart nid -> do res <- addmonitor nid
                                                          return ((),res)
          matchSignal cmd = case cmd of
                               NodeMonitorNodeFailure nid -> do res <- handlefailure nid
                                                                return (res)
          listenaction nid mainpid =
             let onfailure = sendSimple mainpid (NodeMonitorNodeFailure nid)  PldUser >> return ()
                 loop = do res <- pingNode nid
                           case res of
                              False -> onfailure
                              True -> liftIO (threadDelay interpingtimeout) >> loop
             in
               do loop 
          failurelimit = 3 -- TODO make configurable
          interpingtimeout = 5000000
          retrytimeout =  1000000
          reportfailure nid = do mynid <- getSelfNode
                                 sendSimple (adminGetPid mynid ServiceProcessMonitor) (GlNodeDown nid) PldAdmin
          handlefailure nid = case Map.lookup nid state of
                                    Just c -> if c >= failurelimit
                                                then do _ <- reportfailure nid
                                                        return (Map.delete nid state)
                                                else do mypid <- getSelfPid
                                                        _ <- spawnLocalAnd (liftIO (threadDelay retrytimeout) >> listenaction nid mypid) setDaemonic
                                                        return (Map.adjust succ nid state)
                                    Nothing -> return state
          addmonitor nid = case Map.member nid state of
                                  True -> return state
                                  False -> do mynid <- getSelfNode
                                              mypid <- getSelfPid
                                              if mynid==nid
                                                 then return state
                                                 else do _ <- spawnLocalAnd (listenaction nid mypid) setDaemonic
                                                         return $ Map.insert nid (0) state
                                                 
       in receiveWait [roundtripResponse matchCommand,
                       match matchSignal,
                       matchUnknownThrow] >>= service
        

----------------------------------------------
-- * Service helpers
----------------------------------------------

data ServiceId =  
                 ServiceLog 
               | ServiceSpawner 
               | ServiceNodeRegistry 
               | ServiceNodeMonitor 
               | ServiceProcessMonitor 
               | ServiceProcessRegistry
                 deriving (Ord,Eq,Enum,Show)

adminGetPid :: NodeId -> ServiceId -> ProcessId
adminGetPid nid sid = ProcessId nid (fromEnum sid)

adminDeregister :: ServiceId -> ProcessM ()
adminDeregister val = do p <- getProcess
                         pid <- getSelfPid
                         liftIO $ modifyMVar_ (prNodeRef p) (fun $ localFromPid pid)
            where fun pid node = return $ node {ndAdminProcessTable = Map.delete val (ndAdminProcessTable node)}

adminRegister :: ServiceId -> ProcessM ()
adminRegister val =  do p <- getProcess
                        pid <- getSelfPid
                        liftIO $ modifyMVar_ (prNodeRef p) (\node ->
                         if Map.member val (ndAdminProcessTable node) 
                           then throw $ ServiceException $ "Duplicate administrative registration at index " ++ show val    
                           else (fun (localFromPid pid) node))
            where fun pid node = return $ node {ndAdminProcessTable = Map.insert val pid (ndAdminProcessTable node)}

{- UNUSED
adminLookup :: ServiceId -> ProcessM LocalProcessId
adminLookup val = do p <- getProcess
                     node <- liftIO $ readMVar (prNodeRef p)
                     case Map.lookup val (ndAdminProcessTable node) of
                        Nothing -> throw $ ServiceException $ "Request for unknown administrative service " ++ show val
                        Just x -> return x
-}

adminLookupN :: ServiceId -> MVar Node -> IO (Either TransmitStatus LocalProcessId)
adminLookupN val mnode = 
                  do node <- readMVar mnode
                     case Map.lookup val (ndAdminProcessTable node) of
                        Nothing -> return $ Left QteUnknownPid
                        Just x -> return $ Right x

startFinalizerService :: ProcessM () -> ProcessM ()
startFinalizerService todo = spawnLocalAnd body prefix >> return ()
                 where prefix = setDaemonic
                       body = 
                             do p <- getProcess
                                node <- liftIO $ readMVar (prNodeRef p)
                                liftIO $ takeMVar (ndNodeFinalizer node)
                                todo `Control.finally` (liftIO $ putMVar (ndNodeFinalized node) ())


performFinalization :: MVar Node -> IO ()
performFinalization mnode = do node <- readMVar mnode
                               putMVar (ndNodeFinalizer node) ()
                               takeMVar (ndNodeFinalized node)                              

setDaemonic :: ProcessM ()
setDaemonic = do p <- getProcess
                 pid <- getSelfPid
                 liftIO $ modifyMVar_ (prNodeRef p) 
                    (\node -> return $ node {ndProcessTable=Map.adjust (\pte -> pte {pteDaemonic=True}) (localFromPid pid) (ndProcessTable node)})

serviceThread :: ServiceId -> ProcessM () -> ProcessM ()
serviceThread v f = spawnLocalAnd (Control.bracket (return ())
                             (\_ -> adminDeregister v >> logError)
                             (\_ -> f)) (adminRegister v >> setDaemonic)
                        >> (return()) 
          where logError = logS "SYS" LoFatal $ "System process "++show v++" has terminated" -- TODO maybe restart?


----------------------------------------------
-- * Process registry service 
----------------------------------------------


data ProcessRegistryState = ProcessRegistryState (Map.Map String ProcessId) (Map.Map ProcessId String)

data ProcessRegistryCommand = ProcessRegistryQuery String (Maybe (Closure (ProcessM ())))
                            | ProcessRegistrySet String ProcessId deriving (Typeable)
instance Binary ProcessRegistryCommand where
  put (ProcessRegistryQuery a b) = putWord8 0 >> put a >> put b
  put (ProcessRegistrySet a b) = putWord8 1 >> put a >> put b
  get = do cmd <- getWord8
           case cmd of
             0 -> do a <- get
                     b <- get
                     return $ ProcessRegistryQuery a b
             1 -> do a <- get
                     b <- get
                     return $ ProcessRegistrySet a b
data ProcessRegistryAnswer = ProcessRegistryResponse (Maybe ProcessId)
                           | ProcessRegistryError String deriving (Typeable)
instance Binary ProcessRegistryAnswer where
  put (ProcessRegistryResponse a) = putWord8 0 >> put a
  put (ProcessRegistryError s) = putWord8 1 >> put s
  get = do a <- getWord8
           case a of
             0 -> get >>= (return . ProcessRegistryResponse)
             1 -> get >>= (return . ProcessRegistryError)

startProcessRegistryService :: ProcessM ()
startProcessRegistryService = serviceThread ServiceProcessRegistry (service initialState)
  where
    initialState = ProcessRegistryState Map.empty Map.empty
    service state@(ProcessRegistryState nameToPid pidToName) = 
      let
        downs (ProcessMonitorException pid _why) =
          case Map.lookup pid pidToName of
            Just name ->
              let newPidToName = Map.delete pid pidToName
                  newNameToPid = Map.delete name nameToPid
               in return (ProcessRegistryState newNameToPid newPidToName)
            Nothing -> return state
        cmds cmd = 
          case cmd of
            ProcessRegistrySet name pid ->
              case (Map.lookup pid pidToName, Map.lookup name nameToPid) of
                (Nothing,Nothing) -> 
                   let newNameToPid = Map.insert name pid nameToPid
                       newPidToName = Map.insert pid name pidToName
                    in do islocal <- isPidLocal pid
                          case islocal of
                             True -> 
                               do mypid <- getSelfPid
                                  ok <- monitorProcessQuiet mypid pid MaMonitor
                                  case ok of
                                    True -> return (ProcessRegistryResponse Nothing,ProcessRegistryState newNameToPid newPidToName)
                                    False -> return (ProcessRegistryError $ "Couldn't establish monitoring of task in naming "++name,state)
                             False -> return (ProcessRegistryError $ "Refuse to register nonlocal process" ++ show pid,state)
                (Nothing,_) -> return (ProcessRegistryError $ "The name "++name++" has already been registered",state)
                (_,_) -> return (ProcessRegistryError $ "The process "++show pid++" has already been registered",state)
            ProcessRegistryQuery name mClo ->
              case Map.lookup name nameToPid of
                Just pid -> return (ProcessRegistryResponse (Just pid),state)
                Nothing -> 
                  case mClo of
                    Nothing -> return (ProcessRegistryResponse Nothing,state)
                    Just clo -> do mynid <- getSelfNode
                                   mypid <- getSelfPid
                                   pid <- spawnAnd mynid clo defaultSpawnOptions {amsoMonitor=Just (mypid,MaMonitor)}
                                   let newNameToPid = Map.insert name pid nameToPid
                                       newPidToName = Map.insert pid name pidToName
                                   return (ProcessRegistryResponse (Just pid),ProcessRegistryState newNameToPid newPidToName)
       in receiveWait [roundtripResponse cmds,match downs] >>= service


-- TODO nameQueryOrWait :: NodeId -> String -> ProcessM ProcessId

-- | Similar to 'nameQuery' but if the named process doesn't exist,
-- it will be started from the given closure. If the process is
-- already running, the closure will be ignored.
nameQueryOrStart :: NodeId -> String -> Closure (ProcessM ()) -> ProcessM ProcessId
nameQueryOrStart nid name clo =
  let servicepid = adminGetPid nid ServiceProcessRegistry
      msg = ProcessRegistryQuery name (Just clo)
   in do res <- roundtripQueryUnsafe PldAdmin servicepid msg
         case res of
            Right (ProcessRegistryResponse (Just answer)) -> return answer
            Right (ProcessRegistryError s) -> throw $ ServiceException s
            _ -> throw $ ServiceException $ "Crazy talk from process registry"

-- | Query the PID of a named process on a particular node.
-- If no process of that name exists, or if that
-- process has ended, this function returns Nothing.
nameQuery :: NodeId -> String -> ProcessM (Maybe ProcessId)
nameQuery nid name =
  let servicepid = adminGetPid nid ServiceProcessRegistry
      msg = ProcessRegistryQuery name Nothing
   in do res <- roundtripQueryUnsafe PldAdmin servicepid msg
         case res of
            Right (ProcessRegistryResponse answer) -> return answer
            Right (ProcessRegistryError s) -> throw $ ServiceException s
            _ -> throw $ ServiceException $ "Crazy talk from process registry"

-- | Assigns a name to the current process. The name is local to the
-- node. On each node, each process may have only one name, and each
-- name may be given to only one node. If this function is called
-- more than once by the same process, or called more than once
-- with the name on a single node, it will throw a 'ServiceException'.
-- The PID of a named process can be queried later with 'nameQuery'. When the
-- named process ends, its name will again become available.
-- One reason to use named processes is to create node-local state.
-- This example lets each node have its own favorite color, which can
-- be changed and queried.
--
-- > nodeFavoriteColor :: ProcessM ()
-- > nodeFavoriteColor =
-- >  do nameSet "favorite_color"
-- >     loop Blue
-- >  where loop color =
-- >      receiveWait
-- >         [ match (\newcolor -> return newcolor),
-- >           match (\pid -> send pid color >> return color)
-- >         ] >>= loop
-- >
-- > setFavoriteColor :: NodeId -> Color -> ProcessM ()
-- > setFavoriteColor nid color =
-- >  do (Just pid) <- nameQuery nid "favorite_color"
-- >     send pid color
-- >
-- > getFavoriteColor :: NodeId -> ProcessM Color
-- > getFavoriteColor nid =
-- >  do (Just pid) <- nameQuery nid "favorite_color"
-- >     mypid <- getSelfPid
-- >     send pid mypid
-- >     expect
nameSet :: String -> ProcessM ()
nameSet name = 
  do mynid <- getSelfNode
     mypid <- getSelfPid
     let servicepid = adminGetPid mynid ServiceProcessRegistry
         msg = ProcessRegistrySet name mypid
     res <- roundtripQueryLocal PldAdmin servicepid msg
     case res of
        Right (ProcessRegistryResponse Nothing) -> return ()
        Right (ProcessRegistryError s) -> throw $ ServiceException s
        _ -> throw $ ServiceException $ "Crazy talk from process registry"




----------------------------------------------
-- * Global and spawn service 
----------------------------------------------

data GlSignal = GlProcessDown ProcessId SignalReason
              | GlNodeDown NodeId deriving (Typeable,Show)
instance Binary GlSignal where
   put (GlProcessDown a b) = putWord8 0 >> put a >> put b
   put (GlNodeDown a) = putWord8 1 >> put a
   get = do ch <- getWord8
            case ch of
              0 -> do a <- get
                      b <- get
                      return $ GlProcessDown a b
              1 -> do a <- get
                      return $ GlNodeDown a

data GlSynchronous = GlRequestMonitoring ProcessId ProcessId MonitorAction
                   | GlRequestMoniteeing ProcessId NodeId 
                   | GlRequestUnmonitoring ProcessId ProcessId MonitorAction deriving (Typeable)
instance Binary GlSynchronous where
     put (GlRequestMonitoring a b c) = putWord8 0 >> put a >> put b >> put c
     put (GlRequestMoniteeing a b) = putWord8 1 >> put a >> put b
     put (GlRequestUnmonitoring a b c) = putWord8 2 >> put a >> put b >> put c
     get = do ch <- getWord8
              case ch of
                 0 -> do a <- get
                         b <- get
                         c <- get
                         return $  GlRequestMonitoring a b c
                 1 -> do a <- get
                         b <- get
                         return $ GlRequestMoniteeing a b
                 2 -> do a <- get
                         b <- get
                         c <- get
                         return $  GlRequestUnmonitoring a b c

data GlCommand = GlMonitor ProcessId ProcessId MonitorAction
               | GlUnmonitor ProcessId ProcessId MonitorAction deriving (Typeable)
instance Binary GlCommand where
   put (GlMonitor a b c) = putWord8 0 >> put a >> put b >> put c
   put (GlUnmonitor a b c) = putWord8 1 >> put a >> put b >> put c
   get = do ch <- getWord8
            case ch of
               0 -> do a <- get
                       b <- get
                       c <- get
                       return $ GlMonitor a b c
               1 -> do a <- get
                       b <- get
                       c <- get
                       return $ GlUnmonitor a b c

-- | The different kinds of monitoring available between processes.
data MonitorAction = MaMonitor -- ^ MaMonitor means that the monitor process will be sent a ProcessDownException message when the monitee terminates for any reason.
                   | MaLink -- ^ MaLink means that the monitor process will receive an asynchronous exception of type ProcessDownException when the monitee terminates for any reason

                   | MaLinkError -- ^ MaLinkError means that the monitor process will receive an asynchronous exception of type ProcessDownException when the monitee terminates abnormally

                   deriving (Typeable,Show,Ord,Eq)
instance Binary MonitorAction where 
  put MaMonitor = putWord8 0
  put MaLink = putWord8 1
  put MaLinkError = putWord8 2
  get = getWord8 >>= \x ->
        case x of
          0 -> return MaMonitor
          1 -> return MaLink
          2 -> return MaLinkError

-- | Part of the notification system of process monitoring, indicating why the monitor is being notified.
data SignalReason = SrNormal  -- ^ the monitee terminated normally
                  | SrException String -- ^ the monitee terminated with an uncaught exception, which is given as a string
                  | SrNoPing -- ^ the monitee is believed to have ended or be inaccessible, as the node on which its running is not responding to pings. This may indicate a network bisection or that the remote node has crashed.
                  | SrInvalid -- ^ SrInvalid: the monitee was not running at the time of the attempt to establish monitoring
                    deriving (Typeable,Show)
instance Binary SignalReason where 
     put SrNormal = putWord8 0
     put (SrException s) = putWord8 1 >> put s
     put SrNoPing = putWord8 2
     put SrInvalid = putWord8 3
     get = do a <- getWord8
              case a of
                 0 -> return SrNormal
                 1 -> get >>= return . SrException
                 2 -> return SrNoPing
                 3 -> return SrInvalid

type GlLinks = Map.Map ProcessId 
                  (Map.Map (LocalProcessId,MonitorAction) (Int),
                   Map.Map LocalProcessId (Int),
                   Map.Map NodeId ())

data GlobalData = GlobalData
     {
        glLinks :: GlLinks,
        glNextId :: Integer,
        glSyncs :: Map.Map Integer (GlobalData -> MatchM GlobalData ())
     }

gdCombineEntry :: (Map.Map (LocalProcessId,MonitorAction) (Int),
                   Map.Map LocalProcessId (Int),
                   Map.Map NodeId ()) -> (Map.Map (LocalProcessId,MonitorAction) (Int),
                   Map.Map LocalProcessId (Int),
                   Map.Map NodeId ()) -> (Map.Map (LocalProcessId,MonitorAction) (Int),
                   Map.Map LocalProcessId (Int),
                   Map.Map NodeId ())
gdCombineEntry (newmonitors,newmonitees,newnodes) (oldmonitors,oldmonitees,oldnodes) = 
    let finalnodes = Map.unionWith const newnodes oldnodes
        finalmonitors = Map.unionWith (+) newmonitors oldmonitors
        finalmonitees = Map.unionWith (+) newmonitees oldmonitees
     in (finalmonitors,finalmonitees,finalnodes)

gdAddMonitor :: GlLinks -> ProcessId -> MonitorAction -> LocalProcessId -> GlLinks
gdAddMonitor gl pid ma lpid = 
   Map.insertWith' gdCombineEntry pid (Map.singleton (lpid,ma) 1, Map.empty,Map.empty) gl

gdDelMonitor :: GlLinks -> ProcessId -> MonitorAction -> LocalProcessId -> GlLinks
gdDelMonitor gl pid ma lpid = Map.adjust editentry pid gl
  where editentry (mons,mots,ns) = (fixmons mons,mots,ns)
        fixmons m = Map.update (\a -> if pred a == 0
                                         then Nothing
                                         else Just (pred a)) (lpid,ma) m

gdAddMonitee :: GlLinks -> ProcessId -> LocalProcessId -> GlLinks
gdAddMonitee gl pid lpid = 
   Map.insertWith' gdCombineEntry pid (Map.empty,Map.singleton lpid 1,Map.empty) gl

gdDelMonitee :: GlLinks -> ProcessId -> LocalProcessId -> GlLinks
gdDelMonitee gl pid lpid = Map.adjust editentry pid gl
  where editentry (mons,mots,ns) = (mons,fixmons mots,ns)
        fixmons m = Map.update (\a -> if pred a == 0
                                         then Nothing
                                         else Just (pred a)) lpid m

glExpungeProcess :: GlLinks -> ProcessId -> NodeId -> GlLinks
glExpungeProcess gl pid myself = 
                    let mine n = buildPidFromNodeId myself n
                     in case Map.lookup pid gl of
                             Nothing -> gl
                             Just (mons,mots,_ns) -> 
                                  let s1 = Map.delete pid gl
                                      s2 = foldl' (\g (lp,_)-> Map.delete (mine lp) g) s1 (Map.keys mons)
                                      s3 = foldl' (\g lp -> Map.delete (mine lp) g) s2 (Map.keys mots)
                                   in s3

gdAddNode :: GlLinks -> ProcessId -> NodeId -> GlLinks
gdAddNode gl pid nid = Map.insertWith' gdCombineEntry pid (Map.empty,Map.empty,Map.singleton nid ()) gl

-- | The main form of notification to a monitoring process that a monitored process has terminated.
-- This data structure can be delivered to the monitor either as a message (if the monitor is
-- of type 'MaMonitor') or as an asynchronous exception (if the monitor is of type 'MaLink' or 'MaLinkError').
-- It contains the PID of the monitored process and the reason for its nofication.
data ProcessMonitorException = ProcessMonitorException ProcessId SignalReason deriving (Typeable)

instance Binary ProcessMonitorException where
  put (ProcessMonitorException pid sr) = put pid >> put sr
  get = do pid <- get
           sr <- get
           return $ ProcessMonitorException pid sr

instance Exception ProcessMonitorException

instance Show ProcessMonitorException where
  show (ProcessMonitorException pid why) = "ProcessMonitorException: " ++ show pid ++ " has terminated because "++show why

-- | Establishes bidirectional abnormal termination monitoring between the current
-- process and another. Monitoring established with linkProcess
-- is bidirectional and signals only in the event of abnormal termination.
-- In other words, @linkProcess a@ is equivalent to:
--
-- > monitorProcess mypid a MaLinkError
-- > monitorProcess a mypid MaLinkError
linkProcess :: ProcessId -> ProcessM ()
linkProcess p = do mypid <- getSelfPid 
                   mynid <- getSelfNode
                   let servicepid = (adminGetPid mynid ServiceProcessMonitor) 
                       msg1 = GlMonitor mypid p MaLinkError
                       msg2 = GlMonitor p mypid MaLinkError
                   res1 <- roundtripQueryLocal PldAdmin servicepid msg1
                   case res1 of 
                      Right QteOK -> do res2 <- roundtripQueryLocal PldAdmin servicepid msg2
                                        case res2 of 
                                           Right QteOK -> return ()
                                           Right err -> herr err
                                           Left err -> herr err
                      Right err -> herr err
                      Left err -> herr err
        where herr err = throw $ ServiceException $ "Error when linking process " ++ show p ++ ": " ++ show err

-- | A specialized version of 'match' (for use with 'receive', 'receiveWait' and friends) for catching process down
-- messages. This way processes can avoid waiting forever for a response from another process that has crashed.
-- Intended to be used within a 'withMonitor' block, e.g.:
--
-- > withMonitor apid $
-- >   do send apid QueryMsg
-- >      receiveWait 
-- >      [
-- >        match (\AnswerMsg -> return "ok"),
-- >        matchProcessDown apid (return "aborted")   
-- >      ]
matchProcessDown :: ProcessId -> ProcessM q -> MatchM q ()
matchProcessDown pid f = matchIf (\(ProcessMonitorException p _) -> p==pid) (const f)

-- | Establishes temporary monitoring of another process. The process to be monitored is given in the
-- first parameter, and the code to run in the second. If the given process goes down while the code
-- in the second parameter is running, a process down message will be sent to the current process,
-- which can be handled by 'matchProcessDown'. 
withMonitor :: ProcessId -> ProcessM a -> ProcessM a
withMonitor pid f = withMonitoring pid MaMonitor f

withMonitoring :: ProcessId -> MonitorAction -> ProcessM a -> ProcessM a
withMonitoring pid how f =  
                        do mypid <- getSelfPid
                           monitorProcess mypid pid how -- TODO if this throws a ServiceException, translate that into a trigger
                           a <- f `Control.finally` safety (unmonitorProcess mypid pid how)
                           return a
              where safety n = Control.try n :: ProcessM (Either ServiceException ())


-- | Establishes unidirectional processing of another process. The format is:
--
-- > monitorProcess monitor monitee action
--
-- Here,
--
-- * monitor is the process that will be notified if the monitee goes down
--
-- * monitee is the process that will be monitored
--
-- * action determines how the monitor will be notified
--
-- Monitoring will remain in place until one of the processes ends or until
-- 'unmonitorProcess' is called. Calls to 'monitorProcess' are cumulative,
-- such that calling 'monitorProcess' 3 three times on the same pair of processes
-- will ensure that monitoring will stay in place until 'unmonitorProcess' is called
-- three times on the same pair of processes.
-- If the monitee is not currently running, the monitor will be signalled immediately.
-- See also 'MonitorAction'.
monitorProcess :: ProcessId -> ProcessId -> MonitorAction -> ProcessM ()
monitorProcess monitor monitee how = monitorProcessImpl GlMonitor monitor monitee how True >> return ()

-- | Removes monitoring established by 'monitorProcess'. Note that the type of
-- monitoring, given in the third parameter, must match in order for monitoring
-- to be removed. If monitoring has not already been established between these
-- two processes, this function takes not action.
unmonitorProcess :: ProcessId -> ProcessId -> MonitorAction -> ProcessM ()
unmonitorProcess monitor monitee how = monitorProcessImpl GlUnmonitor monitor monitee how True >> return ()

monitorProcessQuiet :: ProcessId -> ProcessId -> MonitorAction -> ProcessM Bool
monitorProcessQuiet monitor monitee how = 
  do res <- monitorProcessImpl GlMonitor monitor monitee how False
     case res of
       QteOK -> return True
       _ -> return False

monitorProcessImpl :: (ProcessId -> ProcessId -> MonitorAction -> GlCommand) -> ProcessId -> ProcessId -> MonitorAction -> Bool -> ProcessM TransmitStatus
monitorProcessImpl msgtype monitor monitee how throwit = 
     let msg = msgtype monitor monitee how
      in do let servicepid = (adminGetPid (nodeFromPid monitee) ServiceProcessMonitor) 
            res <- roundtripQueryUnsafe PldAdmin servicepid msg -- TODO: Prefer to send this message to the local service
            case res of
              Right QteOK -> return QteOK
              Right err -> herr err
              Left err -> herr err
  where herr err = 
            case throwit of
              True -> throw $ ServiceException $ "Error when monitoring process " ++ show monitee ++ ": " ++ show err
              False -> return err

sendInterrupt :: (Exception e) => ProcessId -> e -> ProcessM Bool
sendInterrupt pid e = do islocal <- isPidLocal pid
                         case islocal of
                            False -> return False
                            True -> do p <- getProcess
                                       node <- liftIO $ readMVar (prNodeRef p)
                                       res <- liftIO $ atomically $ getProcessTableEntry node (localFromPid pid)
                                       case res of 
                                          Just pte -> do liftIO $ throwTo (pteThread pte) e
                                                         return True
                                          Nothing -> return False

triggerMonitor :: ProcessId -> ProcessId -> MonitorAction -> SignalReason -> ProcessM ()
triggerMonitor towho aboutwho how why = 
                 let 
                      msg = ProcessMonitorException aboutwho why
                  in case how of
                       MaMonitor -> sendSimple towho msg PldUser  >> return ()
                       MaLink -> sendInterrupt towho msg  >> return ()
                       MaLinkError -> case why of
                                        SrNormal -> return ()
                                        _ -> sendInterrupt towho msg  >> return ()

startProcessMonitorService :: ProcessM ()
startProcessMonitorService = serviceThread ServiceProcessMonitor (service emptyGlobal)
  where 
    emptyGlobal = GlobalData {glLinks=Map.empty, glNextId=0, glSyncs = Map.empty}
    getGlobalFor pid = adminGetPid (nodeFromPid pid) ServiceProcessMonitor
    checkliveness pid = do islocal <- isPidLocal pid
                           case islocal of
                             True -> isProcessUp (localFromPid pid)
                             False -> return True
    checklivenessandtrigger monitor monitee action = 
                                            do a <- checkliveness monitee
                                               case a of
                                                  True -> return True
                                                  False -> do trigger monitor monitee action SrInvalid
                                                              return False
    trigger = triggerMonitor
{- UNUSED
    forward destinationnode msg = sendSimple (getGlobalFor destinationnode) msg PldAdmin
-}
    isProcessUp lpid = do p <- getProcess
                          node <- liftIO $ readMVar (prNodeRef p)
                          res <- liftIO $ atomically $ getProcessTableEntry node lpid
                          case res of
                            Nothing -> return (lpid<0)
                            Just _ -> return True
    removeLocalMonitee gl monitor monitee _action = 
         gl {glLinks = gdDelMonitee (glLinks gl) monitor (localFromPid monitee) }
    removeLocalMonitor gl monitor monitee action =
         gl {glLinks = gdDelMonitor (glLinks gl) monitee action (localFromPid monitor) }
    addLocalMonitee gl monitor monitee _action =
         gl {glLinks = gdAddMonitee (glLinks gl) monitor (localFromPid monitee) }
    addLocalMonitor gl monitor monitee action = 
         gl {glLinks = gdAddMonitor (glLinks gl) monitee action (localFromPid monitor) }
    addLocalNode gl monitor monitee _action =
         gl {glLinks = gdAddNode (glLinks gl) monitee (nodeFromPid monitor)}
    broadcast nids msg = mapM_ (\p -> forkProcessWeak $ ((ptimeout 5000000 $ sendSimple (adminGetPid p ServiceProcessMonitor) msg PldAdmin) >> return ())) nids
    handleProcessDown :: GlLinks -> ProcessId -> SignalReason -> ProcessM GlLinks
    handleProcessDown global pid why = 
                     do islocal <- isPidLocal pid
                        mynid <- getSelfNode
                        case Map.lookup pid global of
                           Nothing -> return global
                           Just (monitors,_monitee,nodes) ->
                               do mapM_ (\(tellwho,how) -> trigger (buildPidFromNodeId mynid tellwho) pid how why) (Map.keys monitors)
                                  when (islocal)
                                    (broadcast (Map.keys nodes) (GlProcessDown pid why))
                                  mynid <- getSelfNode
                                  return $ glExpungeProcess global pid mynid
    service global = 
         let
             additional = map (\receiver -> receiver global) (Map.elems (glSyncs global))
             matchPatterns = [match matchSignals,
                          roundtripResponseAsync matchCommands False,
                          roundtripResponse (matchSyncs global)] ++ additional
             matchSignals cmd = 
                case cmd of
                  GlProcessDown pid why -> 
                     do res <- handleProcessDown (glLinks global) pid why
                        return $ global { glLinks = res}
                  GlNodeDown nid -> let gl = glLinks global
                                        aslist = Map.keys gl
                                        pids = filter (\n -> nodeFromPid n == nid) aslist
                                     in do res <- foldM (\g p -> handleProcessDown g p SrNoPing) gl pids
                                           return $ global {glLinks = res}
             matchSyncs global cmd =
                case cmd of
                  GlRequestMonitoring monitee monitor action ->
                       let s1 = addLocalMonitor global monitor monitee action
                        in monitorNode (nodeFromPid monitee)
                             >> return (QteOK,s1)
                  GlRequestMoniteeing pid towho -> 
                       do live <- checkliveness pid
                          case live of
                              True -> let s1 = global {glLinks = gdAddNode (glLinks global) pid towho}
                                       in return (QteOK,s1)
                              False -> return (QteUnknownPid,global)
                  GlRequestUnmonitoring monitee monitor action ->
                       let s1 = removeLocalMonitor global monitor monitee action
                        in return (QteOK,s1)
             matchCommands cmd ans = 
                case cmd of
                  GlMonitor monitor monitee action -> 
                    do ismoniteelocal <- isPidLocal monitee
                       ismonitorlocal <- isPidLocal monitor
                       case (ismoniteelocal,ismonitorlocal) of
                         (True,True) -> do live <- checklivenessandtrigger monitor monitee action
                                           case live of
                                              True -> let s1 = addLocalMonitee global monitor monitee action
                                                          s2 = addLocalMonitor s1 monitor monitee action
                                                       in ans QteOK >> return s2
                                              False -> ans QteOK >> return global
                         (True,False) -> do live <- checklivenessandtrigger monitor monitee action
                                            case live of
                                              False -> ans QteOK >> return global
                                              True -> let msg = GlRequestMonitoring monitee monitor action
                                                          receiver myId myGlobal myMsg =
                                                             let newGlobal = myGlobal {glSyncs = Map.delete myId (glSyncs myGlobal)}
                                                              in case myMsg of
                                                                   QteOK -> let s1 = addLocalNode newGlobal monitor monitee action
                                                                             in do _ <- ans QteOK
                                                                                   return s1
                                                                   err -> ans err >> return newGlobal
                                                       in do mmatch <- roundtripQueryImplSub PldAdmin (getGlobalFor monitor) msg (receiver (glNextId global))
                                                             case mmatch of
                                                                Left err -> ans err >> return global
                                                                Right mymatch -> return global {glNextId=glNextId global+1,
                                                                                                glSyncs=Map.insert (glNextId global) (mymatch) (glSyncs global)}
                         (False,True) -> let msg = GlRequestMoniteeing monitee (nodeFromPid monitor)
                                             receiver myId myGlobal myMsg =
                                               let newGlobal = myGlobal {glSyncs = Map.delete myId (glSyncs myGlobal)}
                                                in case myMsg of
                                                     QteOK -> let s1 = addLocalMonitor newGlobal monitor monitee action 
                                                               in do monitorNode (nodeFromPid monitee)
                                                                     _ <- ans QteOK
                                                                     return s1 
                                                     QteUnknownPid -> do trigger monitor monitee action SrInvalid
                                                                         _ <- ans QteOK
                                                                         return newGlobal
                                                     err -> do _ <- ans err
                                                               return newGlobal
                                          in do mmatch <- roundtripQueryImplSub PldAdmin (getGlobalFor monitee) msg (receiver (glNextId global))
                                                case mmatch of
                                                   Left err -> ans err >> return global
                                                   Right mymatch -> return global {glNextId=glNextId global+1,
                                                                                   glSyncs=Map.insert (glNextId global) (mymatch) (glSyncs global)}
                         (False,False) -> do _ <- ans (QteOther "Requesting monitoring by third party node")
                                             return global
                  GlUnmonitor monitor monitee action -> 
                    do ismoniteelocal <- isPidLocal monitee
                       ismonitorlocal <- isPidLocal monitor
                       case (ismoniteelocal,ismonitorlocal) of
                         (True,True) -> let s1 = removeLocalMonitee global monitor monitee action 
                                            s2 = removeLocalMonitor s1 monitor monitee action
                                         in ans QteOK >> return s2
                         (True,False) -> let msg = GlRequestUnmonitoring monitee monitor action
                                             receiver myId myGlobal myMsg =
                                                let newGlobal = myGlobal {glSyncs=Map.delete myId (glSyncs myGlobal)}
                                                 in ans myMsg >> return newGlobal

                                          in do sync <- roundtripQueryImplSub PldAdmin (getGlobalFor monitee) msg (receiver (glNextId global))
                                                case sync of
                                                   Left err -> ans err >> return global
                                                   Right mymatch -> return global {glNextId=glNextId global+1,
                                                                                   glSyncs=Map.insert (glNextId global) (mymatch) (glSyncs global)} 
                         (False,True) -> let s1 = removeLocalMonitor global monitor monitee action
                                          in ans QteOK >> return s1
                         (False,False) -> ans (QteOther "Requesting unmonitoring by third party node") >> return global
          in receiveWait matchPatterns >>= service

data AmSpawn = AmSpawn (Closure (ProcessM ())) AmSpawnOptions deriving (Typeable)
instance Binary AmSpawn where 
    put (AmSpawn c o) =put c >> put o
    get=get >>= \c -> get >>= \o -> return $ AmSpawn c o
data AmCall = AmCall ProcessId (Closure Payload) deriving (Typeable)
instance Binary AmCall where
    put (AmCall pid clo) = put pid >> put clo
    get = do c <- get
             d <- get
             return $ AmCall c d

data AmSpawnOptions = AmSpawnOptions 
        { 
          amsoPaused :: Bool,
          amsoLink :: Maybe ProcessId,
          amsoMonitor :: Maybe (ProcessId,MonitorAction),
          amsoName :: Maybe String
        } deriving (Typeable) 
instance Binary AmSpawnOptions where
    put (AmSpawnOptions a b c d) = put a >> put b >> put c >> put d
    get = do a <- get
             b <- get
             c <- get
             d <- get
             return $ AmSpawnOptions a b c d

defaultSpawnOptions :: AmSpawnOptions
defaultSpawnOptions = AmSpawnOptions {amsoPaused=False, amsoLink=Nothing, amsoMonitor=Nothing, amsoName=Nothing}

data AmSpawnUnpause = AmSpawnUnpause deriving (Typeable)
instance Binary AmSpawnUnpause where
   put AmSpawnUnpause = return ()
   get = return AmSpawnUnpause

-- TODO. Of course, this would need to be in a different module.
-- spawnWithChannel :: NodeId -> Closure (ReceivePort a -> ProcessM ()) -> ProcessM SendPort
-- spawnWithChannel nid clo = send ...

-- | Start a process running the code, given as a closure, on the specified node.
-- If successful, returns the process ID of the new process. If unsuccessful,
-- throw a 'TransmitException'. 
spawn :: NodeId -> Closure (ProcessM ()) -> ProcessM ProcessId
spawn node clo = spawnAnd node clo defaultSpawnOptions

-- | Ends the current process in an orderly manner.
terminate :: ProcessM a
terminate = throw ProcessTerminationException

-- | If a remote process has been started in a paused state with 'spawnAnd' ,
-- it will be running but inactive until unpaused. Use this function to unpause
-- such a function. It has no effect on processes that are not paused or that
-- have already been unpaused.
unpause :: ProcessId -> ProcessM ()
unpause pid = send pid AmSpawnUnpause

-- | A variant of 'spawn' that starts the remote process with
-- bidirectoinal monitoring, as in 'linkProcess'
spawnLink :: NodeId -> Closure (ProcessM ()) -> ProcessM ProcessId
spawnLink node clo = do mypid <- getSelfPid
                        spawnAnd node clo defaultSpawnOptions {amsoLink=Just mypid}

-- | A variant of 'spawn' that allows greater control over how the remote process is started.
spawnAnd :: NodeId -> Closure (ProcessM ()) -> AmSpawnOptions -> ProcessM ProcessId
spawnAnd node clo opt = 
    do res <- roundtripQueryUnsafe PldAdmin (adminGetPid node ServiceSpawner) (AmSpawn clo opt) 
       case res of
         Left e -> throw $ TransmitException e
         Right pid -> return pid	

-- | Invokes a function on a remote node. The function must be
-- given by a closure. This function will block until the called
-- function completes or the connection is broken.
callRemote :: (Serializable a) => NodeId -> Closure (ProcessM a) -> ProcessM a
callRemote node clo = callRemoteImpl node clo

callRemoteIO :: (Serializable a) => NodeId -> Closure (IO a) -> ProcessM a
callRemoteIO node clo = callRemoteImpl node clo

callRemotePure :: (Serializable a) => NodeId -> Closure a -> ProcessM a
callRemotePure node clo = callRemoteImpl node clo

callRemoteImpl :: (Serializable a) => NodeId -> Closure b -> ProcessM a
callRemoteImpl node clo = 
    let newclo = makePayloadClosure clo
     in case newclo of
          Nothing -> throw $ TransmitException QteUnknownCommand
          Just plclo ->
               do mypid <- getSelfPid
                  res <- roundtripQuery PldAdmin (adminGetPid node ServiceSpawner) (AmCall mypid plclo)
                  case res of
                     Right (Just mval) -> 
                       do val <- liftIO $ serialDecode mval
                          case val of
                             Just a -> return a
                             _ -> throw $ TransmitException QteUnknownCommand
                     Left e -> throw (TransmitException e)
                     _ -> throw $ TransmitException QteUnknownCommand


startSpawnerService :: ProcessM ()
startSpawnerService = serviceThread ServiceSpawner spawner
   where spawner = receiveWait [matchSpawnRequest,matchCallRequest,matchUnknownThrow] >> spawner
         exceptFilt :: SomeException -> q -> q
         exceptFilt _ q = q
         callWorker c responder = do a <- Control.try $ invokeClosure c
                                     case a of
                                        Left q -> exceptFilt q (responder Nothing)
                                        Right Nothing -> responder Nothing
                                        Right (Just pl) -> responder (Just pl)
         spawnWorker c = do a <- invokeClosure c
                            case a of
                                Nothing -> (logS "SYS" LoCritical $ "Failed to invoke closure "++(show c)) --TODO it would be nice if this error could be propagated to the caller of spawn, at the very least it should throw an exception so a linked process will be notified 
                                Just q -> q
         matchCallRequest = roundtripResponseAsync 
               (\cmd sender -> case cmd of
                    AmCall _pid clo -> spawnLocal (callWorker clo sender) >> return ()) False
         matchSpawnRequest = roundtripResponse 
               (\cmd -> case cmd of
                    AmSpawn c opt -> 
                      let 
                        namePostlude = case amsoName opt of
                                         Nothing -> return ()
                                         Just name -> nameSet name
                        pausePrelude = case amsoPaused opt of
                                            False -> return ()
                                            True -> receiveWait [match (\AmSpawnUnpause -> return ())]
                        linkPostlude = case amsoLink opt of
                                            Nothing -> return ()
                                            Just pid -> linkProcess pid
                        monitorPostlude = case amsoMonitor opt of
                                            Nothing -> return ()
                                            Just (pid,ma) -> do mypid <- getSelfPid
                                                                _ <- monitorProcessQuiet pid mypid ma
                                                                return ()
                      in do newpid <- spawnLocalAnd (pausePrelude >> spawnWorker c) (namePostlude >> linkPostlude >> monitorPostlude)
                            return (newpid,()))


----------------------------------------------
-- * Local node registry
----------------------------------------------

{- UNUSED
localRegistryMagicProcess :: LocalProcessId
localRegistryMagicProcess = 38813
-}

localRegistryMagicMagic :: String
localRegistryMagicMagic = "__LocalRegistry"

localRegistryMagicRole :: String
localRegistryMagicRole = "__LocalRegistry"

type LocalProcessName = String

-- | Created by 'Remote.Peer.getPeers', this maps
-- each role to a list of nodes that have that role.
-- It can be examined directly or queried with
-- 'findPeerByRole'.
type PeerInfo = Map.Map String [NodeId]

data LocalNodeData = LocalNodeData {ldmRoles :: PeerInfo} 

{- UNUSED
type RegistryData = Map.Map String LocalNodeData
-}

data LocalProcessMessage =
        LocalNodeRegister String String NodeId
      | LocalNodeUnregister String String NodeId
      | LocalNodeQuery String
      | LocalNodeAnswer PeerInfo
      | LocalNodeResponseOK
      | LocalNodeResponseError String
      | LocalNodeHello
        deriving (Typeable)

instance Binary LocalProcessMessage where
   put (LocalNodeRegister m r n) = putWord8 0 >> put m >> put r >> put n
   put (LocalNodeUnregister m r n) = putWord8 1 >> put m >> put r >> put n
   put (LocalNodeQuery r) = putWord8 3 >> put r
   put (LocalNodeAnswer pi) = putWord8 4 >> put pi
   put (LocalNodeResponseOK) = putWord8 5
   put (LocalNodeResponseError s) = putWord8 6 >> put s
   put (LocalNodeHello) = putWord8 7
   get = do g <- getWord8
            case g of
              0 -> get >>= \m -> get >>= \r -> get >>= \n -> return (LocalNodeRegister m r n)
              1 -> get >>= \m -> get >>= \r -> get >>= \n -> return (LocalNodeUnregister m r n)
              3 -> get >>= return . LocalNodeQuery
              4 -> get >>= return . LocalNodeAnswer
              5 -> return LocalNodeResponseOK
              6 -> get >>= return . LocalNodeResponseError
              7 -> return LocalNodeHello

remoteRegistryPid :: NodeId -> ProcessM ProcessId
remoteRegistryPid nid =
     do let (NodeId hostname _) = nid
        cfg <- getConfig
        return $ adminGetPid (NodeId hostname (cfgLocalRegistryListenPort cfg)) ServiceNodeRegistry

localRegistryPid :: ProcessM ProcessId
localRegistryPid =
     do (NodeId hostname _) <- getSelfNode
        cfg <- getConfig
        return $ adminGetPid (NodeId hostname (cfgLocalRegistryListenPort cfg)) ServiceNodeRegistry

-- | Every host on which a node is running also needs a node registry,
-- which arbitrates those nodes can responds to peer queries. If
-- no registry is running, one will be automatically started
-- when the framework is started, but the registry can be started
-- independently, also. This function does that.
standaloneLocalRegistry :: String -> IO ()
standaloneLocalRegistry cfgn = do cfg <- readConfig True (Just cfgn)
                                  res <- startLocalRegistry cfg True
                                  liftIO $ putStrLn $ "Terminating standalone local registry: " ++ show res

-- | Contacts the local node registry and attempts to register current node. 
-- You probably don't want to call this function yourself, as it's done for you in 'Remote.Init.remoteInit'
localRegistryRegisterNode :: ProcessM ()
localRegistryRegisterNode = localRegistryRegisterNodeImpl LocalNodeRegister

-- | Contacts the local node registry and attempts to unregister current node. 
-- You probably don't want to call this function yourself, as it's done for you in 'Remote.Init.remoteInit'
localRegistryUnregisterNode :: ProcessM ()
localRegistryUnregisterNode = localRegistryRegisterNodeImpl LocalNodeUnregister

localRegistryRegisterNodeImpl :: (String -> String -> NodeId -> LocalProcessMessage) -> ProcessM ()
localRegistryRegisterNodeImpl cons = 
    do cfg <- getConfig
       lrpid <- localRegistryPid
       nid <- getSelfNode
       let regMsg = cons (cfgNetworkMagic cfg) (cfgRole cfg) nid
       res <- roundtripQueryUnsafe PldAdmin lrpid regMsg
       case res of
          Left ts -> throw $ TransmitException ts
          Right LocalNodeResponseOK -> return ()
          Right (LocalNodeResponseError s) -> throw $ TransmitException $ QteOther s

-- | Contacts the local node registry and attempts to verify that it is alive.
-- If the local node registry cannot be contacted, an exception will be thrown.
localRegistryHello :: ProcessM ()
localRegistryHello = do lrpid <- localRegistryPid
                        res <- roundtripQueryUnsafe PldAdmin lrpid LocalNodeHello 
                        case res of
                           (Right LocalNodeHello) -> return ()
                           (Left n) -> throw $ ConfigException $ "Can't talk to local node registry: " ++ show n
                           _ -> throw $ ConfigException $ "No response from local node registry"

localRegistryQueryNodes :: NodeId -> ProcessM (Maybe PeerInfo)
localRegistryQueryNodes nid = 
    do cfg <- getConfig
       lrpid <- remoteRegistryPid nid
       let regMsg = LocalNodeQuery (cfgNetworkMagic cfg)
       res <- roundtripQueryUnsafe PldAdmin lrpid regMsg
       case res of
         Left _ts -> return Nothing
         Right (LocalNodeAnswer pi) -> return $ Just pi

-- TODO since local registries are potentially sticky, there is good reason
-- to ensure that they don't get corrupted; there should be some
-- kind of security precaution here, e.g. making sure that registrations
-- only come from local nodes
startLocalRegistry :: Config -> Bool -> IO TransmitStatus
startLocalRegistry cfg waitforever = startit
 where
  regConfig = cfg {cfgListenPort = cfgLocalRegistryListenPort cfg, cfgNetworkMagic=localRegistryMagicMagic, cfgRole = localRegistryMagicRole}
  handler tbl = receiveWait [roundtripResponse (registryCommand tbl)] >>= handler
  emptyNodeData = LocalNodeData {ldmRoles = Map.empty}
  lookup tbl magic role = case Map.lookup magic tbl of
                                 Nothing -> Nothing
                                 Just ldm -> Map.lookup role (ldmRoles ldm)
  remove tbl magic role nid =
             let
              roler Nothing = Nothing
              roler (Just lst) = Just $ filter ((/=)nid) lst
              removeFrom ldm = ldm {ldmRoles = Map.alter (roler) role (ldmRoles ldm)}
              remover Nothing = Nothing
              remover (Just ldm) = Just (removeFrom ldm)
             in Map.alter remover magic tbl
  insert tbl magic role nid = 
             let
              roler Nothing = Just [nid]
              roler (Just lst) = if elem nid lst
                                    then (Just lst)
                                    else (Just (nid:lst))
              insertTo ldm = ldm {ldmRoles = Map.alter (roler) role (ldmRoles ldm)}
                                
              inserter Nothing = Just (insertTo emptyNodeData)
              inserter (Just ldm) = Just (insertTo ldm)
             in Map.alter inserter magic tbl
  registryCommand tbl (LocalNodeQuery magic) = 
             case Map.lookup magic tbl of
                Nothing -> return (LocalNodeAnswer Map.empty,tbl)
                Just pi -> return (LocalNodeAnswer (ldmRoles pi),tbl)
  registryCommand tbl (LocalNodeUnregister magic role nid) =     -- check that given entry exists?
             return (LocalNodeResponseOK,remove tbl magic role nid)
  registryCommand tbl (LocalNodeRegister magic role nid) =
             case lookup tbl magic role of
                Nothing -> return (LocalNodeResponseOK,insert tbl magic role nid)
                Just nids -> if elem nid nids 
                                then return (LocalNodeResponseError ("Multiple node registration for " ++ show nid ++ " as " ++ role),tbl)
                                else return (LocalNodeResponseOK,insert tbl magic role nid)
  registryCommand tbl (LocalNodeHello) = return (LocalNodeHello,tbl)
  registryCommand tbl _ = return (LocalNodeResponseError "Unknown command",tbl)
  startit = do node <- initNode regConfig Remote.Reg.empty
               res <- try $ forkAndListenAndDeliver node regConfig :: IO (Either SomeException ())
               case res of
                  Left e -> return $ QteNetworkError $ show e
                  Right () -> runLocalProcess node (startLoggingService >> 
                                                    adminRegister ServiceNodeRegistry >> 
                                                    handler Map.empty) >>
                              if waitforever 
                                 then waitForThreads node >>
                                      (return $ QteOther "Local registry process terminated")
                                 else return QteOK


----------------------------------------------
-- * Closures
----------------------------------------------

-- TODO makeClosure should verify that the given fun argument actually exists
makeClosure :: (Typeable a,Serializable v) => String -> v -> ProcessM (Closure a)
makeClosure fun env = do 
                         enc <- liftIO $ serialEncode env 
                         return $ Closure fun enc

makePayloadClosure :: Closure a -> Maybe (Closure Payload)
makePayloadClosure (Closure name arg) = 
                case isSuffixOf "__impl" name of
                  False -> Nothing
                  True -> Just $ Closure (name++"Pl") arg

evaluateClosure :: (Typeable b) => Closure a -> ProcessM (Maybe (Payload -> b))
evaluateClosure (Closure name _) =
        do node <- getLookup
           return $ getEntryByIdent node name

invokeClosure :: (Typeable a) => Closure a -> ProcessM (Maybe a)
invokeClosure (Closure name arg) = 
           (\_id ->
                do node <- getLookup
                   res <- sequence [pureFun node,ioFun node,procFun node]
                   case catMaybes res of
                      (a:_) -> return $ Just a
                      _ -> return Nothing ) Prelude.id
   where pureFun node = case getEntryByIdent node name of
                          Nothing -> return Nothing
                          Just x -> return $ Just $ (x arg)
         ioFun node =   case getEntryByIdent node name of
                          Nothing -> return Nothing
                          Just x -> liftIO (x arg) >>= (return.Just)
         procFun node = case getEntryByIdent node name of
                          Nothing -> return Nothing
                          Just x -> (x arg) >>= (return.Just)


----------------------------------------------
-- * Simple functional queue
----------------------------------------------

data Queue a = Queue [a] [a]

queueMake :: Queue a
queueMake = Queue [] []

{- UNUSED
queueEmpty :: Queue a -> Bool
queueEmpty (Queue [] []) = True
queueEmpty _ = False
-}

queueInsert :: Queue a -> a -> Queue a
queueInsert (Queue incoming outgoing) a = Queue (a:incoming) outgoing

{- UNUSED
queueInsertAndLimit :: Queue a -> Int -> a -> Queue a
queueInsertAndLimit q limit a= 
       let s1 = queueInsert q a
           s2 = if queueLength s1 > limit
                   then let (_,f) = queueRemove s1
                         in f
                   else s1
        in s2
-}

queueInsertMulti :: Queue a -> [a] -> Queue a
queueInsertMulti (Queue incoming outgoing) a = Queue (a++incoming) outgoing

{- UNUSED
queueRemove :: Queue a -> (Maybe a,Queue a)
queueRemove (Queue incoming (a:outgoing)) = (Just a,Queue incoming outgoing)
queueRemove (Queue l@(_:_) []) = queueRemove $ Queue [] (reverse l)
queueRemove q@(Queue [] []) = (Nothing,q)
-}

queueToList :: Queue a -> [a]
queueToList (Queue incoming outgoing) = outgoing ++ reverse incoming
queueFromList :: [a] -> Queue a
queueFromList l = Queue [] l


{- UNUSED
queueLength :: Queue a -> Int
queueLength (Queue incoming outgoing) = length incoming + length outgoing -- should probably just store in the length in the structure

queueEach :: Queue a -> [(a,Queue a)]
queueEach q = case queueToList q of
                     [] -> []
                     a:rest -> each [] a rest
        where each before it []                     = [(it,queueFromList before)]
              each before it following@(next:after) = (it,queueFromList (before++following)):(each (before++[it]) next after)
-}

withSem :: QSem -> IO a -> IO a
withSem sem f = action `finally` signalQSem sem
   where action = waitQSem sem >> f


