{-# LANGUAGE DeriveDataTypeable #-}
module Remote.Process where

import Control.Concurrent (forkIO,ThreadId,threadDelay)
import Control.Concurrent.MVar (MVar,newMVar, newEmptyMVar,isEmptyMVar,takeMVar,putMVar,modifyMVar_,readMVar)
import Control.Exception (ErrorCall(..),throwTo,bracket,try,Exception,throw,evaluate,finally,SomeException,PatternMatchFail(..))
import Control.Monad (foldM,when,liftM)
import Control.Monad.Trans (MonadTrans,lift,MonadIO,liftIO)
import Data.Binary (Binary,put,get,putWord8,getWord8)
import Data.Char (isSpace,isDigit)
import Data.Generics (Data)
import Data.List (foldl', isPrefixOf,nub)
import Data.Maybe (listToMaybe,catMaybes,fromJust,isNothing)
import Data.Typeable (Typeable)
import Data.Unique (newUnique,hashUnique)
import System.IO (Handle,hClose,hSetBuffering,hGetChar,hPutChar,BufferMode(..),hFlush)
import System.IO.Error (isEOFError,isDoesNotExistError,isUserError)
import Network.BSD (HostEntry(..),getHostName,getHostEntries)
import Network (HostName,PortID(..),PortNumber(..),listenOn,accept,sClose,connectTo,Socket)
import Network.Socket (PortNumber(..),setSocketOption,SocketOption(..),socketPort,aNY_PORT )
import qualified Data.Map as Map (Map,fromList,elems,member,map,empty,adjust,alter,insert,delete,lookup,toList,size,insertWith')
import Remote.Call (getEntryByIdent,Lookup,empty)
import Remote.Encoding (serialEncode,serialDecode,serialEncodePure,serialDecodePure,Payload,Serializable,PayloadLength,genericPut,genericGet,hPutPayload,hGetPayload,payloadLength,getPayloadType)
import System.Environment (getArgs)
import qualified System.Timeout (timeout)
import System.FilePath (FilePath)
import Data.Time (getCurrentTime,diffUTCTime,UTCTime(..),utcToLocalZonedTime)
import Remote.Closure (Closure (..))
import Control.Concurrent.STM (STM,atomically,retry,orElse)
import Control.Concurrent.STM.TChan (TChan,isEmptyTChan,readTChan,newTChanIO,writeTChan)
import Control.Concurrent.STM.TVar (TVar,newTVarIO,readTVar,writeTVar)



import Debug.Trace

----------------------------------------------
-- * Closures
----------------------------------------------

decode :: (Serializable a) => Payload -> ProcessM (Maybe a)
decode pl = do 
               liftIO $ serialDecode pl

-- TODO makeClosure should verify that the given fun argument actually exists
makeClosure :: (Typeable a,Serializable v) => String -> v -> ProcessM (Closure a)
makeClosure fun env = do 
                         enc <- liftIO $ serialEncode env 
                         return $ Closure fun enc

invokeClosure :: (Typeable a) => Closure a -> ProcessM (Maybe a)
invokeClosure (Closure name arg) = (\id ->
                do node <- getLookup
                   res <- sequence [pureFun node,ioFun node,procFun node]
                   case catMaybes res of
                      (a:b) -> return $ Just a
                      _ -> return Nothing ) id
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
queueEmpty :: Queue a -> Bool
queueEmpty (Queue [] []) = True
queueEmpty _ = False

queueInsert :: Queue a -> a -> Queue a
queueInsert (Queue incoming outgoing) a = Queue (a:incoming) outgoing

queueInsertMulti :: Queue a -> [a] -> Queue a
queueInsertMulti (Queue incoming outgoing) a = Queue (a++incoming) outgoing

queueRemove :: Queue a -> (Maybe a,Queue a)
queueRemove (Queue incoming (a:outgoing)) = (Just a,Queue incoming outgoing)
queueRemove (Queue l@(_:_) []) = queueRemove $ Queue [] (reverse l)
queueRemove q@(Queue [] []) = (Nothing,q)

queueToList :: Queue a -> [a]
queueToList (Queue incoming outgoing) = outgoing ++ reverse incoming
queueFromList :: [a] -> Queue a
queueFromList l = Queue [] l

queueEach :: Queue a -> [(a,Queue a)]
queueEach q = case queueToList q of
                     [] -> []
                     a:rest -> each [] a rest
        where each before it []                     = [(it,queueFromList before)]
              each before it following@(next:after) = (it,queueFromList (before++following)):(each (before++[it]) next after)

----------------------------------------------
-- * Process monad
----------------------------------------------

type PortId = Int
type LocalProcessId = Int

data Config = Config 
         {
             cfgRole :: !String,
             cfgHostName :: !HostName,
             cfgListenPort :: !PortId,
             cfgLocalRegistryListenPort :: !PortId,
             cfgPeerDiscoveryPort :: !PortId,
             cfgNetworkMagic :: !String,
             cfgKnownHosts :: ![String],
--             cfgRoundtripTimeout :: Int -- currently unused
--             logConfig :: LogConfig
             cfgArgs :: ![String]
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
           ndLogConfig :: LogConfig
           --conncetion table -- each one is MVared
           --  also contains time info about last contact
       }


data NodeId = NodeId HostName PortId deriving (Data,Typeable,Eq)

data ProcessId = ProcessId NodeId LocalProcessId deriving (Data,Typeable,Eq) -- TODO OR find-by-processname

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

data Message = Message
   {
       msgDisposition :: PayloadDisposition,
       msgHeader :: Maybe Payload,
       msgPayload :: Payload
   }

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
       prLogConfig :: Maybe LogConfig
   } 
                           

data ProcessM a = ProcessM {runProcessM :: Process -> IO (Process,a)} deriving Typeable

instance Monad ProcessM where
    m >>= k = ProcessM $ (\p -> (runProcessM m) p >>= (\(news,newa) -> runProcessM (k newa) news))
    return x = ProcessM $ \s -> return (s,x)

instance Functor ProcessM where
    fmap f v = ProcessM $ (\p -> (runProcessM v) p >>= (\x -> return $ fmap f x))

instance MonadIO ProcessM where
    liftIO arg = ProcessM $ \pr -> (arg >>= (\x -> return (pr,x)))

getProcess :: ProcessM (Process)
getProcess = ProcessM $ \x -> return (x,x)

getConfig :: ProcessM (Config)
getConfig = do p <- getProcess
               node <- liftIO $ readMVar (prNodeRef p)
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
                                     Just pte -> do msg <- readTChan (pteChannel pte)
                                                    return $ Just msg
                                     Nothing -> return Nothing
                      

getCurrentMessages :: Process -> STM [Message]
getCurrentMessages p = do
                         msgs <- cleanChannel (prChannel p) []
                         ps <- readTVar (prState p)
                         let q = (prQueue ps) 
                         let newq = queueInsertMulti q msgs
                         writeTVar (prState p) ps {prQueue = newq}
                         return $ queueToList newq
     where cleanChannel c m = do empty <- isEmptyTChan c
                                 if empty 
                                    then return m
                                    else do item <- readTChan c
                                            cleanChannel c (item:m)
                        
matchMessage :: [MatchM q ()] -> Message -> STM (Maybe (ProcessM q))
matchMessage matchers msg = do (mb,r) <- (foldl orElse (retry) (map executor matchers)) `orElse` (return (theMatchBlock,Nothing))
                               return r
   where executor x = do 
                         (ok@(mb,matchfound),_) <- runMatchM x theMatchBlock
                         case matchfound of
                            Nothing -> retry
                            n -> return ok
         theMatchBlock = MatchBlock {mbMessage = msg}

matchMessages :: [MatchM q ()] -> [(Message,STM ())] -> STM (Maybe (ProcessM q))
matchMessages matchers msgs = (foldl orElse (retry) (map executor msgs)) `orElse` (return Nothing)
   where executor (msg,acceptor) = do
                                      res <- matchMessage matchers msg
                                      case res of
                                         Nothing -> retry
                                         Just pmq -> acceptor >> return (Just pmq)

receive :: [MatchM q ()] -> ProcessM (Maybe q)
receive m = do p <- getProcess
               res <- liftIO $ atomically $ 
                          do
                             msgs <- getCurrentMessages p
                             matchMessages m (mkMsgs p msgs)
               case res of
                  Nothing -> return Nothing
                  Just n -> do q <- n
                               return $ Just q
      where mkMsgs p msgs = map (\(m,q) -> (m,do ps <- readTVar (prState p)
                                                 writeTVar (prState p) ps {prQueue = queueFromList q})) (exclusionList msgs)

receiveWait :: [MatchM q ()] -> ProcessM q
receiveWait m = 
            do p <- getProcess
               v <- attempt1 p
               attempt2 p v
      where mkMsgs p msgs = map (\(m,q) -> (m,do ps <- readTVar (prState p)
                                                 writeTVar (prState p) ps {prQueue = queueFromList q})) (exclusionList msgs)
            attempt2 p v = 
                case v of
                  Just n -> n
                  Nothing -> do ret <- liftIO $ atomically $ 
                                         do 
                                            msg <- getNewMessage p
                                            ps <- readTVar (prState p)
                                            let oldq = prQueue ps
                                            writeTVar (prState p) ps {prQueue = queueInsert oldq msg}
                                            matchMessages m [(msg,writeTVar (prState p) ps)]
                                attempt2 p ret
            attempt1 p = liftIO $ atomically $ 
                          do
                             msgs <- getCurrentMessages p
                             matchMessages m (mkMsgs p msgs)

receiveTimeout :: Int -> [MatchM q ()] -> ProcessM (Maybe q)
receiveTimeout to m = ptimeout to $ receiveWait m

matchUnknown :: ProcessM q -> MatchM q ()
matchUnknown body = returnHalt () body

matchUnknownThrow :: MatchM q ()
matchUnknownThrow = do mb <- getMatch
                       returnHalt () (throw $ UnknownMessageException (getPayloadType $ msgPayload (mbMessage mb)))

match :: (Serializable a) => (a -> ProcessM q) -> MatchM q ()
match = matchCoreHeaderless PldUser (const True)

matchIf :: (Serializable a) => (a -> Bool) -> (a -> ProcessM q) -> MatchM q ()
matchIf = matchCoreHeaderless PldUser

matchCoreHeaderless :: (Serializable a) => PayloadDisposition -> (a -> Bool) -> (a -> ProcessM q) -> MatchM q ()
matchCoreHeaderless pld f g = matchCore pld (\(a,b) -> b==(Nothing::Maybe ()) && f a)
                                            (\(a,_) -> g a)

matchCore :: (Serializable a,Serializable b) => PayloadDisposition -> ((a,Maybe b) -> Bool) -> ((a,Maybe b) -> ProcessM q) -> MatchM q ()
matchCore pld cond body = 
        do mb <- getMatch
           doit mb
 where 
  doit mb = 
        let    
           decodified = serialDecodePure (msgPayload (mbMessage mb))
           decodifiedh = maybe (Nothing) (serialDecodePure) (msgHeader (mbMessage mb))
        in
           case decodified of
             Just x -> if cond (x,decodifiedh) 
                          then returnHalt () (body (x,decodifiedh))
                          else liftSTM retry
             Nothing -> liftSTM retry



----------------------------------------------
-- * Exceptions and return values
----------------------------------------------

data ConfigException = ConfigException String deriving (Show,Typeable)
instance Exception ConfigException

data TransmitException = TransmitException TransmitStatus deriving (Show,Typeable)
instance Exception TransmitException

data UnknownMessageException = UnknownMessageException String deriving (Show,Typeable)
instance Exception UnknownMessageException

data ServiceException = ServiceException String deriving (Show,Typeable)
instance Exception ServiceException

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
                    | QteUnknownCommand deriving (Show,Read)


data ProcessMonitorException = ProcessMonitorException ProcessId SignalType SignalReason deriving (Typeable)

instance Exception ProcessMonitorException

instance Show ProcessMonitorException where
  show (ProcessMonitorException pid typ why) = "ProcessMonitorException propagated from process " ++ show pid ++ " by signal " ++ show typ ++ ", reason "++show why


----------------------------------------------
-- * Node and process spawning
----------------------------------------------

initNode :: Config -> Lookup -> IO (MVar Node)
initNode cfg lookup = 
               do defaultHostName <- getHostName
--                  hostEntries <- getHostEntries False
                  let newHostName =
                       if null (cfgHostName cfg)
                          then defaultHostName
                          else cfgHostName cfg -- TODO it would be nice to check that is name actually makes sense, e.g. try to contact ourselves
                  theNewHostName <- evaluate newHostName
                  mvar <- newMVar Node {ndHostName=theNewHostName, 
--                                        ndHostEntries=hostEntries, 
                                        ndProcessTable=Map.empty,
                                        ndAdminProcessTable=Map.empty,
                                        ndConfig=cfg,
                                        ndLookup=lookup,
                                        ndListenPort=0,
                                        ndLogConfig=defaultLogConfig}
                  return mvar

roleDispatch :: MVar Node -> (String -> ProcessM ()) -> IO ()
roleDispatch mnode func = readMVar mnode >>= 
              \node -> 
                   let role = cfgRole (ndConfig node) in
                   runLocalProcess mnode (func role) >> return ()

spawnAnd :: ProcessM () -> ProcessM () -> ProcessM ProcessId
spawnAnd fun and = do p <- getProcess
                      v <- liftIO $ newEmptyMVar
                      pid <- liftIO $ runLocalProcess (prNodeRef p) (myFun v)
                      liftIO $ takeMVar v
                      return pid
   where myFun mv = (and `pfinally` liftIO (putMVar mv ())) >> fun

-- | A synonym for 'spawn'
forkProcess :: ProcessM () -> ProcessM ProcessId
forkProcess = spawn

-- | Create a parallel process sharing the same message queue and PID.
-- Not safe for export, as doing any message receive operation could
-- result in a munged message queue. 
forkProcessWeak :: ProcessM () -> ProcessM ()
forkProcessWeak f = do p <- getProcess
                       res <- liftIO $ forkIO (runProcessM f p >> return ())
                       return ()

spawn :: ProcessM () -> ProcessM ProcessId
spawn fun = do p <- getProcess
               liftIO $ runLocalProcess (prNodeRef p) fun

runLocalProcess :: MVar Node -> ProcessM () -> IO ProcessId
runLocalProcess node fun = 
         do 
             localprocessid <- newLocalProcessId
             channel <- newTChanIO
             passProcess <- newEmptyMVar
             okay <- newEmptyMVar
             thread <- forkIO (runner okay node passProcess)
             thenodeid <- getNodeId node
             pid <- evaluate $ ProcessId thenodeid localprocessid
             state <- newTVarIO $ ProcessState {prQueue = queueMake}
             putMVar passProcess (mkProcess channel node localprocessid thread pid state)
             takeMVar okay
             return pid
         where
          notifyProcessDown p r = do nid <- getNodeId node
                                     let pp = adminGetPid nid ServiceGlobal 
                                     let msg = AmSignal (prPid p) SigProcessDown r True
                                     try $ sendBasic node pp (msg) (Nothing::Maybe ()) PldAdmin :: IO (Either SomeException TransmitStatus)--ignore result ok
          notifyProcessUp p = do nid <- getNodeId node
                                 let pp = adminGetPid nid ServiceGlobal 
                                 let msg = AmSignal (prPid p) SigProcessUp SrNormal True
                                 try $ sendBasic node pp (msg) (Nothing::Maybe ()) PldAdmin :: IO (Either SomeException TransmitStatus)--ignore result ok
          exceptionHandler e p = let shown = show e in
                notifyProcessDown (p) (SrException shown) >>
                 (try (logI node  (prPid p) "SYS" LoCritical (concat ["Process got unhandled exception ",shown]))::IO(Either SomeException ())) >> return () --ignore error
          exceptionCatcher p fun = do notifyProcessUp (p)
                                      res <- try fun -- TODO handle interprocess signals separately
                                      case res of
                                        Left e -> exceptionHandler (e::SomeException) p
                                        Right a -> notifyProcessDown (p) SrNormal >> return a
          runner okay node passProcess = do
                  p <- takeMVar passProcess
                  let init = do death <- newEmptyMVar
                                death2 <- newTVarIO False
                                let pte = (mkProcessTableEntry (prChannel p) (prThread p) death death2)
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
          mkProcess channel noderef localprocessid thread pid state =
             Process
                {
                  prPid = pid,
                  prSelf = localprocessid,
                  prChannel = channel,
                  prNodeRef = noderef,
                  prThread = thread,
                  prState = state,
                  prLogConfig = Nothing
                }


----------------------------------------------
-- * Roundtrip conversations
----------------------------------------------

roundtripQueryAsync :: (Serializable a,Serializable b) => PayloadDisposition -> [ProcessId] -> a -> ProcessM [Either TransmitStatus b]
roundtripQueryAsync pld pids dat = 
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
                                 matcher c = matchCore pld (\(_,h) -> case h of
                                                                       Just a -> (msgheaderConversationId a,msgheaderSender a) `elem` convopids
                                                                       Nothing -> False)
                                                         (\(b,Just h) -> 
                                                               let newmap = Map.adjust (\_ -> (Just (Right b))) (msgheaderConversationId h) c
                                                                in return (newmap) )
                             m <- receiving (Map.fromList res)
                             return $ catMaybes (Map.elems m)

roundtripQuery :: (Serializable a, Serializable b) => PayloadDisposition -> ProcessId -> a -> ProcessM (Either TransmitStatus b)
roundtripQuery pld pid dat =
    do convId <- liftIO $ newConversationId
       sender <- getSelfPid
       res <- sendTry pid dat (Just RoundtripHeader {msgheaderConversationId = convId,msgheaderSender = sender,msgheaderDestination = pid}) pld
       case res of
            QteOK -> receiveWait [matchCore pld (\(_,h) -> case h of
                                                            Just a -> msgheaderConversationId a == convId && msgheaderSender a == pid
                                                            Nothing -> False) (return . Right . fst)]
            err -> return (Left err)

roundtripResponse :: (Serializable a, Serializable b) => PayloadDisposition -> (a -> ProcessM (b,q)) -> MatchM q ()
roundtripResponse pld f =
       matchCore pld (\(_,h)->conditional h) transformer
   where conditional :: Maybe RoundtripHeader -> Bool
         conditional a = case a of
                           Just _ -> True
                           Nothing -> False
         transformer (m,Just h) = 
                         do
                            selfpid <- getSelfPid
                            (a,q) <- f m -- TODO put this in a try block, maybe send error message to other side 
                            res <- sendTry (msgheaderSender h) a (Just RoundtripHeader {msgheaderSender=msgheaderDestination h,msgheaderDestination = msgheaderSender h,
                                                                                 msgheaderConversationId=msgheaderConversationId h}) PldUser
                            case res of
                              QteOK -> return q
                              _ -> throw $ TransmitException res

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

send :: (Serializable a) => ProcessId -> a -> ProcessM ()
send pid msg = sendSimple pid msg PldUser >>= 
                                           (\x -> case x of
                                                    QteOK -> return ()
                                                    _ -> throw $ TransmitException x
                                          )

sendSimple :: (Serializable a) => ProcessId -> a -> PayloadDisposition -> ProcessM TransmitStatus
sendSimple pid dat pld = sendTry pid dat (Nothing :: Maybe ()) pld

sendTry :: (Serializable a,Serializable b) => ProcessId -> a -> Maybe b -> PayloadDisposition -> ProcessM TransmitStatus
sendTry pid msg msghdr pld = getProcess >>= (\p -> 
       let
          tryAction = ptry action >>=
                    (\x -> case x of
                              Left l -> return $ QteEncodingError $ show (l::ErrorCall) -- catch errors from encoding
                              Right r -> return r)
               where action = 
                      do p <- getProcess
                         node <- liftIO $ readMVar (prNodeRef p)
                         liftIO $ sendBasic (prNodeRef p) pid msg msghdr pld
       in tryAction )      

sendBasic :: (Serializable a,Serializable b) => MVar Node -> ProcessId -> a -> Maybe b -> PayloadDisposition -> IO TransmitStatus
sendBasic mnode pid msg msghdr pld = do
              nid <- getNodeId mnode
              node <- readMVar mnode
              encoding <- liftIO $ serialEncode msg
              header <- maybe (return Nothing) (\x -> do t <- liftIO $ serialEncode x
                                                         return $ Just t) msghdr
              let themsg = Message {msgDisposition=pld,msgHeader = header,msgPayload=encoding}
              let islocal = nodeFromPid pid == nid
              (if islocal then sendRawLocal else sendRawRemote) mnode pid nid themsg

sendRawLocal :: MVar Node -> ProcessId -> NodeId -> Message -> IO TransmitStatus
sendRawLocal noderef thepid nodeid msg
     | thepid == nullPid = return QteUnknownPid
     | otherwise = do node <- readMVar noderef
                      messageHandler (ndConfig node) noderef (msgDisposition msg) msg (cfgNetworkMagic (ndConfig node)) (localFromPid thepid)
                  
sendRawRemote :: MVar Node -> ProcessId -> NodeId -> Message -> IO TransmitStatus
sendRawRemote noderef thepid@(ProcessId (NodeId hostname portid) localpid) nodeid msg
     | thepid == nullPid = return QteUnknownPid
     | otherwise = try setup 
                          >>= (\x -> case x of
                                         Right n -> return n
                                         Left l | isEOFError l -> return $ QteNetworkError (show l)
                                                | isUserError l -> return QteBadFormat
                                                | isDoesNotExistError l -> return $ QteNetworkError (show l)
                                                | otherwise -> return $ QteOther $ show l
                                      )
    where setup = bracket (connectTo hostname (PortNumber $ toEnum portid) >>=
               (\h -> hSetBuffering h (BlockBuffering Nothing) >> return h))
                 (hClose)
                 (sender)
          sender h = do node <- readMVar noderef
                        writeMessage h (cfgNetworkMagic (ndConfig node),localpid,nodeid,msg)
    
writeMessage :: Handle -> (String,LocalProcessId,NodeId,Message)-> IO TransmitStatus
writeMessage h (magic,dest,nodeid,msg) = 
         do hPutStrZ h $ unwords ["Rmt!!",magic,show dest,show (msgDisposition msg),show fmt,show nodeid]
            hFlush h
            response <- hGetLineZ h
            resp <- readIO response :: IO TransmitStatus
            case resp of
               QtePleaseSendBody -> do maybe (return ()) (hPutPayload h) (msgHeader msg)
                                       hPutPayload h $ msgPayload msg
                                       hFlush h
                                       response2 <- hGetLineZ h
                                       resp2 <- readIO response2 :: IO TransmitStatus
                                       return resp2
               QteOK -> return QteBadFormat
               n -> return n                
         where fmt = case msgHeader msg of
                        Nothing -> (0::Int)
                        Just _ -> 2

----------------------------------------------
-- * Message delivery
----------------------------------------------

forkAndListenAndDeliver :: MVar Node -> Config -> IO ()
forkAndListenAndDeliver node cfg = do coord <- newEmptyMVar
                                      forkIO $ listenAndDeliver node cfg (coord)
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
                        return (magic,adestp,anodeid,Message { msgHeader = header,
                                           msgDisposition = adisp,
                                           msgPayload = body
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
         do node <- readMVar tbl
            atomically $ do pte <- getProcessTableEntry node adestp
                            maybe (return Nothing) (\x -> writeTChan (pteChannel x) msg >> return (Just x)) pte
                               
listenAndDeliver :: MVar Node -> Config -> MVar (Maybe IOError) -> IO ()
listenAndDeliver node cfg coord = 
-- this listenon should be replaced with a lower-level listen call that binds
-- to the interface corresponding to the name specified in cfgNodeName
          do tryout <- try (listenOn whichPort) :: IO (Either IOError Socket) 
             case tryout of
                  Left e -> putMVar coord (Just e)
                  Right sock -> bracket (
                      do
                        setSocketOption sock KeepAlive 1
                        realPort <- socketPort sock
                        modifyMVar_ node (\a -> return $ a {ndListenPort=fromEnum realPort}) 
                        putMVar coord Nothing
                        return sock)
                     (sClose)
                     (handleConnection)
   where  
         whichPort = if cfgListenPort cfg /= 0
                        then PortNumber $ toEnum $ cfgListenPort cfg
                        else PortNumber aNY_PORT
         handleCommSafe h ho po = 
            try (handleComm h ho po) >>= (\x -> case x of
              Left l -> case l of
                           n | isEOFError n -> writeResult h $ QteNetworkError (show n)
                             | isUserError n -> writeResult h QteBadFormat
                             | otherwise -> writeResult h (QteOther $ show n)
              Right False -> return ()
              Right True -> handleCommSafe h ho po)
         cleanBody n = reverse $ dropWhile isSpace (reverse (dropWhile isSpace n))
         handleComm h hostname portn = 
            do (magic,adestp,nodeid,msg) <- readMessage h
               nd <- readMVar node
               res <- messageHandler cfg node (msgDisposition msg) msg magic adestp
               writeResult h res
               case res of
                 QteOK -> return True
                 _ -> return False

         handleConnection sock = 
            do
               (h,hostname,portn) <- accept sock
               hSetBuffering h (BlockBuffering Nothing)
               forkIO (handleCommSafe h hostname portn `finally` hClose h)
               handleConnection sock


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
                 do tbl <- readMVar node
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
                     do tbl <- readMVar node
                        res <- deliver adestp node msg
                        case res of
                           Nothing -> return QteUnknownPid
                           Just _ -> return QteOK
            | otherwise = return QteBadNetworkMagic

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

newLocalProcessId :: IO LocalProcessId
newLocalProcessId = do d <- newUnique
                       return $ hashUnique d

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

duration :: Int -> ProcessM a -> ProcessM (Int,a)
duration t a = 
           do time1 <- liftIO $ getCurrentTime
              result <- a
              time2 <- liftIO $ getCurrentTime
              return (t - picosecondsToMicroseconds (fromEnum (diffUTCTime time2 time1)),result)
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

getSelfNode :: ProcessM NodeId
getSelfNode = do (ProcessId n p) <- getSelfPid
                 return n

getNodeId :: MVar Node -> IO NodeId
getNodeId mnode = do node <- readMVar mnode
                     return (NodeId (ndHostName node) (ndListenPort node))

getSelfPid :: ProcessM ProcessId
getSelfPid = do p <- getProcess
                case prPid p of
                  (ProcessId (NodeId _ 0) _) -> (buildPid >> getProcess >>= return.prPid)
                  _ -> return $ prPid p

nodeFromPid :: ProcessId -> NodeId
nodeFromPid (ProcessId nid _) = nid

localFromPid :: ProcessId -> LocalProcessId
localFromPid (ProcessId _ lid) = lid

buildPidFromNodeId :: NodeId -> LocalProcessId -> ProcessId
buildPidFromNodeId n lp = ProcessId n lp

localServiceToPid :: LocalProcessId -> ProcessM ProcessId
localServiceToPid sid = do (ProcessId nid lid) <- getSelfPid
                           return $ ProcessId nid sid

isPidLocal :: ProcessId -> ProcessM Bool
isPidLocal pid = do mine <- getSelfPid
                    return (nodeFromPid mine == nodeFromPid pid)


----------------------------------------------
-- * Exception handling
----------------------------------------------

ptry :: (Exception e) => ProcessM a -> ProcessM (Either e a)
ptry f = do p <- getProcess
            res <- liftIO $ try (runProcessM f p)
            case res of
              Left e -> return $ Left e
              Right (newp,newanswer) -> ProcessM (\_ -> return (newp,Right newanswer))

ptimeout :: Int -> ProcessM a -> ProcessM (Maybe a)
ptimeout t f = do p <- getProcess
                  res <- liftIO $ System.Timeout.timeout t (runProcessM f p)
                  case res of
                    Nothing -> return Nothing
                    Just (newp,newanswer) -> ProcessM (\_ -> return (newp,Just newanswer))

pbracket :: (ProcessM a) -> (a -> ProcessM b) -> (a -> ProcessM c) -> ProcessM c
pbracket before after fun = 
       do p <- getProcess
          (newp2,newanswer2) <- liftIO $ bracket 
                         (runProcessM before p) 
                         (\(newp,newanswer) -> runProcessM (after newanswer) newp) 
                         (\(newp,newanswer) -> runProcessM (fun newanswer) newp)
          ProcessM (\_ -> return (newp2, newanswer2))   

pfinally :: ProcessM a -> ProcessM b -> ProcessM a
pfinally fun after = pbracket (return ()) (\_ -> after) (const fun)


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
                  cfgArgs = []
--                  cfgRoundtripTimeout = 500000
                  }

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
--  updateCfg cfg "cfgRoundtripTimeout" p = cfg {cfgRoundtripTimeout=(read.isInt) p}
  updateCfg cfg "cfgLocalRegistryListenPort" p = cfg {cfgLocalRegistryListenPort=(read.isInt) p}
  updateCfg cfg "cfgKnownHosts" m = cfg {cfgKnownHosts=words m}
  updateCfg cfg "cfgNetworkMagic" m = cfg {cfgNetworkMagic=clean m}
  updateCfg _   opt _ = error ("Unknown configuration option: "++opt)
  isInt s | all isDigit s = s
  isInt s = error ("Not a good number: "++s)
  nonempty s | (not.null) s = s
  nonempty b = error ("Unexpected empty item: " ++ b)
  clean = filter (not.isSpace)

----------------------------------------------
-- * Logging
----------------------------------------------

data LogLevel = LoSay
              | LoFatal 
              | LoCritical 
              | LoImportant
              | LoStandard
              | LoInformation
              | LoTrivial deriving (Eq,Ord,Enum,Show)

instance Binary LogLevel where
   put n = put $ fromEnum n
   get = get >>= return . toEnum

type LogSphere = String

data LogTarget = LtStdout 
               | LtForward NodeId 
               | LtFile FilePath 
               | LtForwarded  -- special value -- don't set this in your logconfig!
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

data LogConfig = LogConfig
     {
       logLevel :: LogLevel,
       logTarget :: LogTarget,
       logFilter :: LogFilter
     } deriving (Typeable)

instance Binary LogConfig where
   put (LogConfig ll lt lf) = put ll >> put lt >> put lf
   get = do ll <- get
            lt <- get
            lf <- get
            return $ LogConfig ll lt lf

data LogMessage = LogMessage UTCTime LogLevel LogSphere String ProcessId LogTarget
                | LogUpdateConfig LogConfig deriving (Typeable)
   
instance Binary UTCTime where 
    put (UTCTime a b) = put (fromEnum a) >> put (fromEnum b)
    get = do a <- get
             b <- get
             return $ UTCTime (toEnum a) (toEnum b)
        

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

showLogMessage :: LogMessage -> IO String
showLogMessage (LogMessage utc ll ls s pid _) = 
   do gmt <- utcToLocalZonedTime utc
      return $ concat [paddedString 30 (show gmt)," ",
                       (show $ fromEnum ll)," ",
                       paddedString 28 (show pid)," ",ls," ",s]


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

say :: String -> ProcessM ()
say v = logS "SAY" LoSay v

getLogConfig :: ProcessM LogConfig
getLogConfig = do p <- getProcess
                  case (prLogConfig p) of
                    Just lc -> return lc
                    Nothing -> do node <- liftIO $ readMVar (prNodeRef p)
                                  return (ndLogConfig node)

setLogConfig :: LogConfig -> ProcessM ()
setLogConfig lc = do p <- getProcess
                     putProcess (p {prLogConfig = Just lc})

setNodeLogConfig :: LogConfig -> ProcessM ()
setNodeLogConfig lc = do p <- getProcess
                         liftIO $ modifyMVar_ (prNodeRef p) (\x -> return $ x {ndLogConfig = lc})

setRemoteNodeLogConfig :: NodeId -> LogConfig -> ProcessM ()
setRemoteNodeLogConfig nid lc = do res <- sendSimple (adminGetPid nid ServiceLog) (LogUpdateConfig lc) PldAdmin
                                   case res of 
                                     QteOK -> return ()
                                     n -> throw $ TransmitException $ QteLoggingError

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
                                       in sendBasic mnode svc msg (Nothing::Maybe()) PldAdmin
                                case res of
                                    _ -> return () -- ignore error -- what can I do?
                                
                                  

logS :: LogSphere -> LogLevel -> String -> ProcessM ()
logS sph ll txt = do lc <- getLogConfig
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
                      n -> throw $ TransmitException $ QteLoggingError

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
              LtFile fp -> (ptry (liftIO (appendFile fp (smsg ++ "\n"))) :: ProcessM (Either IOError ()) ) >> return () -- ignore error - what can we do?
              LtForward nid -> do self <- getSelfNode
                                  when (self /= nid) 
                                    (sendSimple (adminGetPid nid ServiceLog) (forwardify txt) PldAdmin >> return ()) -- ignore error -- what can we do?
              n -> throw $ ConfigException $ "Invalid message forwarded setting"
                                  

----------------------------------------------
-- * Global registry
----------------------------------------------

data ServiceId = ServiceGlobal 
               | ServiceLog 
               | ServiceSpawner 
               | ServiceNodeRegistry deriving (Ord,Eq,Enum,Show)

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
                        node <- liftIO $ readMVar (prNodeRef p)
                        if Map.member val (ndAdminProcessTable node) 
                           then throw $ ServiceException $ "Duplicate administrative registration at index " ++ show val    
                           else liftIO $ modifyMVar_ (prNodeRef p) (fun $ localFromPid pid)
            where fun pid node = return $ node {ndAdminProcessTable = Map.insert val pid (ndAdminProcessTable node)}

adminLookup :: ServiceId -> ProcessM LocalProcessId
adminLookup val = do p <- getProcess
                     node <- liftIO $ readMVar (prNodeRef p)
                     case Map.lookup val (ndAdminProcessTable node) of
                        Nothing -> throw $ ServiceException $ "Request for unknown administrative service " ++ show val
                        Just x -> return x

adminLookupN :: ServiceId -> MVar Node -> IO (Either TransmitStatus LocalProcessId)
adminLookupN val mnode = 
                  do node <- readMVar mnode
                     case Map.lookup val (ndAdminProcessTable node) of
                        Nothing -> return $ Left QteUnknownPid
                        Just x -> return $ Right x

setDaemonic :: ProcessM ()
setDaemonic = do p <- getProcess
                 pid <- getSelfPid
                 liftIO $ modifyMVar_ (prNodeRef p) 
                    (\node -> return $ node {ndProcessTable=Map.adjust (\pte -> pte {pteDaemonic=True}) (localFromPid pid) (ndProcessTable node)})

serviceThread :: ServiceId -> ProcessM () -> ProcessM ()
serviceThread v f = spawnAnd (pbracket (return ())
                             (\_ -> adminDeregister v >> logError)
                             (\_ -> f)) (adminRegister v >> setDaemonic) >> return ()
          where logError = logS "SYS" LoFatal $ "System process "++show v++" has terminated" -- TODO maybe restart?

data AmSignal = AmSignal ProcessId SignalType SignalReason Bool deriving (Typeable)
instance Binary AmSignal where 
   put (AmSignal pid st sr p) = put pid >> put st >> put sr >> put p
   get = do pid <- get
            st <- get
            sr <- get
            p <- get
            return $ AmSignal pid st sr p

data AmMonitor = AmMonitor SignalType MonitorAction ProcessId ProcessId Bool deriving(Typeable,Show)
instance Binary AmMonitor where
   put (AmMonitor st ma pid1 pid2 p) = put st >> put ma >> put pid1 >> put pid2 >> put p
   get = do st <- get
            ma <- get
            pid1 <- get
            pid2 <- get
            p <- get
            return $ AmMonitor st ma pid1 pid2 p

data SignalType = SigProcessUp 
                | SigProcessDown
                | SigUser String deriving (Typeable,Eq,Show)
instance Binary SignalType where 
   put SigProcessUp = putWord8 0
   put SigProcessDown = putWord8 1
   put (SigUser n) = putWord8 2 >> put n
   get = getWord8 >>= \x ->
           case x of
             0 -> return SigProcessUp 
             1 -> return SigProcessDown
             2 -> get >>= \q -> return $ SigUser q

data MonitorAction = MaMonitor 
                   | MaLink 
                   | MaLinkError deriving (Typeable,Show)
instance Binary MonitorAction where 
  put MaMonitor = putWord8 0
  put MaLink = putWord8 1
  put MaLinkError = putWord8 2
  get = getWord8 >>= \x ->
        case x of
          0 -> return MaMonitor
          1 -> return MaLink
          2 -> return MaLinkError

data SignalReason = SrNormal | SrException String deriving (Typeable,Data,Show)
instance Binary SignalReason where 
     put SrNormal = putWord8 0
     put (SrException s) = putWord8 1 >> put s
     get = do a <- getWord8
              case a of
                 0 -> return SrNormal
                 1 -> get >>= return . SrException

data AmSpawn = AmSpawn (Closure (ProcessM ())) AmSpawnOptions deriving (Typeable)
instance Binary AmSpawn where 
    put (AmSpawn c o) =put c >> put o
    get=get >>= \c -> get >>= \o -> return $ AmSpawn c o

data AmSpawnOptions = AmSpawnNormal | AmSpawnPaused | AmSpawnMonitor (ProcessId,SignalType,MonitorAction) | AmSpawnLinked ProcessId
instance Binary AmSpawnOptions where
    put AmSpawnNormal = putWord8 0
    put AmSpawnPaused = putWord8 1
    put (AmSpawnMonitor a) = putWord8 2 >> put a
    put (AmSpawnLinked a) = putWord8 3 >> put a
    get = getWord8 >>= 
           \w -> case w of
                   0 -> return AmSpawnNormal
                   1 -> return AmSpawnPaused
                   2 -> get >>= \a -> return $ AmSpawnMonitor a
                   3 -> get >>= \a -> return $ AmSpawnLinked a

data ProcessMonitorMessage = ProcessMonitorMessage ProcessId SignalType SignalReason deriving (Typeable)
instance Binary ProcessMonitorMessage where
   put (ProcessMonitorMessage pid st sr) = put pid >> put st >> put sr
   get = do pid <- get
            st <- get
            sr <- get
            return $ ProcessMonitorMessage pid st sr

data AmSpawnUnpause = AmSpawnUnpause deriving (Typeable)
instance Binary AmSpawnUnpause where
   put AmSpawnUnpause = return ()
   get = return AmSpawnUnpause

spawnRemote :: NodeId -> Closure (ProcessM ()) -> ProcessM ProcessId
spawnRemote node clo = spawnRemoteAnd node clo AmSpawnNormal

spawnRemoteUnpause :: ProcessId -> ProcessM ()
spawnRemoteUnpause pid = send pid AmSpawnUnpause

spawnRemoteAnd :: NodeId -> Closure (ProcessM ()) -> AmSpawnOptions -> ProcessM ProcessId
spawnRemoteAnd node clo opt = 
    do res <- roundtripQuery PldAdmin (adminGetPid node ServiceSpawner) (AmSpawn clo opt)
       case res of
         Left e -> throw $ TransmitException e
         Right pid -> return pid	

startSpawnerService :: ProcessM ()
startSpawnerService = serviceThread ServiceSpawner spawner
   where spawner = receiveWait [matchSpawnRequest,matchUnknownThrow] >> spawner
         matchSpawnRequest = roundtripResponse PldAdmin 
               (\(AmSpawn c opt) -> 
                 case opt of 
                    AmSpawnNormal -> do pid <- spawn (spawnWorker c)
                                        return (pid,())
                    AmSpawnPaused -> do pid <- spawn ((receiveWait [match (\AmSpawnUnpause -> return ())]) >> spawnWorker c)
                                        return (pid,())
                    AmSpawnMonitor (monitorpid,typ,ac) -> 
                                     do pid <- spawnAnd (spawnWorker c)
                                                 (getSelfPid >>= \pid -> monitorProcess monitorpid pid typ ac) -- TODO this needs to be wrapped in case of error
                                        return (pid,())
                    AmSpawnLinked monitorpid -> 
                                     do pid <- spawnAnd (spawnWorker c)
                                                 (linkProcess monitorpid)
                                        return (pid,()))
         spawnWorker c = do a <- invokeClosure c
                            case a of
                                Nothing -> (logS "SYS" LoCritical $ "Failed to invoke closure "++(show c)) --TODO it would be nice if this error could be propagated to the caller of spawnRemote, at the very least it should throw an exception so a linked process will be notified 
                                Just q -> q

linkProcess :: ProcessId -> ProcessM ()
linkProcess p = do me <- getSelfPid
                   monitorProcess me p SigProcessDown MaLinkError
                   monitorProcess p me SigProcessDown MaLinkError

monitorProcess :: ProcessId -> ProcessId -> SignalType -> MonitorAction -> ProcessM ()
monitorProcess monitor monitee which how =
      let msg = (AmMonitor which how monitor monitee True) in
        do islocal <- isPidLocal monitor
           res <- roundtripQuery PldAdmin (adminGetPid (nodeFromPid (if islocal then monitor else monitee)) ServiceGlobal) msg
           case res of
             Right True -> return ()
             Right False -> throw $ TransmitException $ QteOther $ "Unknown error setting monitor in monitorProcess"
             Left t -> throw $ TransmitException $ t

data GlobalProcessEntry = GlobalProcessEntry
     {
          gpeMonitoring :: [(ProcessId,SignalType,MonitorAction)],
          gpeMonitoredBy :: [(ProcessId,SignalType)]
     } deriving (Show)

type GlobalProcessRegistry = Map.Map LocalProcessId GlobalProcessEntry 

data Global = Global
     {
          processRegistry :: GlobalProcessRegistry
     } deriving (Show)

startGlobalService :: ProcessM ()
startGlobalService = serviceThread ServiceGlobal (service emptyGlobal)
  where 
    emptyGlobal = Global {processRegistry=Map.empty}
    service global = 
     let
        matchMonitor2 = matchCoreHeaderless PldAdmin (const True) 
                 (\a -> case a of
                           (AmMonitor sigtype how monitor monitee propogate) -> setMonitor sigtype how monitor monitee propogate >>= return . snd)
                
        matchMonitor = roundtripResponse PldAdmin 
                 (\a -> case a of
                           (AmMonitor sigtype how monitor monitee propogate) -> setMonitor sigtype how monitor monitee propogate)
        matchSignal = 
             matchCoreHeaderless PldAdmin (const True) 
                 (\a -> case a of
                           (AmSignal pid sigtype why propogate) -> processSignal pid sigtype why propogate)
        getAllPids g = concat $ map (\x -> map (\(p,_) -> p) (gpeMonitoredBy x)) (Map.elems (processRegistry g))
        getInterestedPids g pid = 
                 case Map.lookup (localFromPid pid) (processRegistry g) of
                   Nothing -> []
                   Just gpe -> map (\(pp,_,_) -> pp) (gpeMonitoring gpe) ++ map (\(pp,_) -> pp) (gpeMonitoredBy gpe)
        installMonitee (bb,g) pid f@(monitor,sigtype) =
            let adjustor (Just (GlobalProcessEntry a b)) = Just $ GlobalProcessEntry a (adjustor1 b)
                adjustor Nothing = adjustor (Just $ GlobalProcessEntry [] [])
                adjustor1 [] = [f]
                adjustor1 (q@(hmon,hsig):r) = if hmon==monitor && hsig==sigtype
                                                 then q:r
                                                 else q:(adjustor1 r)
                in (bb && ( Map.member (localFromPid pid) (processRegistry g)),g {processRegistry = Map.alter adjustor (localFromPid pid) (processRegistry g)})
        installMonitor (bb,g) pid f@(monitee,sigtype,how) =
            let adjustor (Just (GlobalProcessEntry a b)) = Just $ GlobalProcessEntry (adjustor1 a) b
                adjustor Nothing = adjustor (Just $ GlobalProcessEntry [] [])
                adjustor1 [] = [f]
                adjustor1 (q@(hmon,hsig,how):r) = if hmon==monitee && hsig==sigtype
                                                  then f:r -- or throw
                                                  else q:(adjustor1 r)
                in (bb && ( Map.member (localFromPid pid) (processRegistry g)), g {processRegistry = Map.alter adjustor (localFromPid pid) (processRegistry g)})

        setMonitor sigtype how monitor monitee False =
          do isMonitorLocal <- isPidLocal monitor
             isMoniteeLocal <- isPidLocal monitee
             if (not (isMonitorLocal || isMoniteeLocal))
                then return (False,global)
                else let t1 = if isMonitorLocal
                                 then installMonitor (True,global) monitor (monitee,sigtype,how)
                                 else (True,global)
                         t2 = if isMoniteeLocal
                                 then installMonitee t1 monitee (monitor,sigtype)
                                 else t1
                     in return t2
        setMonitor sigtype how monitor monitee True = 
          do (okay1,newg) <- setMonitor sigtype how monitor monitee False
             isMonitorLocal <- isPidLocal monitor
             isMoniteeLocal <- isPidLocal monitee        
             if okay1
                then if (not isMonitorLocal || not isMoniteeLocal)
                        then let othernode = if isMonitorLocal 
                                             then nodeFromPid monitee
                                             else nodeFromPid monitor
                                 other = adminGetPid othernode ServiceGlobal            
                             in do res <- roundtripQuery PldAdmin other (AmMonitor sigtype how monitor monitee False)
                                   case res of 
                                     Right True -> return (True,newg)
                                     _ -> return (False,global)
                     else return (okay1,newg)
                else return (False,global)
        processSignal :: ProcessId -> SignalType -> SignalReason -> Bool -> ProcessM Global
        processSignal pid SigProcessUp why False = 
            do notify pid SigProcessUp why
               return $ global { processRegistry = Map.insert (localFromPid pid) (GlobalProcessEntry {gpeMonitoring=[],gpeMonitoredBy=[]}) (processRegistry global) } 
        processSignal pid SigProcessDown why False = let
                 removeStuff :: GlobalProcessRegistry -> GlobalProcessRegistry
                 removeStuff a = Map.map takeOut (Map.delete (localFromPid pid) a)
                 takeOut q = q {gpeMonitoring = filter (\(x,_,_) -> x /= pid) (gpeMonitoring q), 
                                gpeMonitoredBy = filter (\(x,_) -> x /= pid) (gpeMonitoredBy q)}
               in
               do notify pid SigProcessDown why
                  return $ global {processRegistry = removeStuff $ processRegistry global}
        processSignal pid sigtype why False =
               notify pid sigtype why >> return global
        processSignal pid sigtype why True = 
             let 
                 sign = (AmSignal pid sigtype why False)
                 pids = if sigtype == SigProcessUp -- special case for process up: monitoring any process on a node will get signals for all new processups on that noe
                           then getAllPids global
                           else getInterestedPids global pid
                 tellwho = filter ((/=) (nodeFromPid pid)) (nub $ map (nodeFromPid) $ pids)
            in do 
                  q <- processSignal pid sigtype why False
                  res <- broadcast sign tellwho -- TODO handle error in some way
                  return q
        monitors x pid = filter (\(p,_,_) -> pid==p) (gpeMonitoring x)

 -- TODO in broadcast, make this concurrent, and log bad results; THIS LINE NEEDS A TIMEOUT OR SOMETHING; get timout value from config, or, even better, integrate directly into roundtripQuery variant
        broadcast msg towho = foldM (\p x -> do query <- ptimeout 500000 (roundtripQuery PldAdmin (adminGetPid x ServiceGlobal) msg) :: ProcessM (Maybe (Either TransmitStatus Bool)) 
                                                case query of
                                                   Nothing -> return False
                                                   Just (Left _) -> return False --log error
                                                   Just (Right r) -> return r) True towho

        sendNotification towho aboutwho typ how reason = 
               let  
                  abnormal = case reason of
                               SrNormal -> False
                               _ -> True
                  sendAsynchException = 
                           do p <- getProcess
                              node <- liftIO $ readMVar (prNodeRef p)
                              case Map.lookup towho (ndProcessTable node) of
                                Nothing -> logS "SYS" LoInformation $ "Process linked from " ++ show aboutwho ++ " can't be notified because it's already gone"
                                Just pte -> liftIO $ throwTo (pteThread pte)
                                              (ProcessMonitorException aboutwho typ reason)
               in
               case how of
                 MaLinkError -> when (abnormal) sendAsynchException
                 MaLink -> sendAsynchException
                 MaMonitor -> do
                       towhom <- localServiceToPid towho
                       res <- sendSimple towhom (ProcessMonitorMessage aboutwho typ reason) PldUser
                       case res of
                          QteOK -> return ()
                          o -> logS "SYS" LoInformation $ "Process "++show towhom++" monitoring " ++ show aboutwho ++ " can't be notified: "++show o
        notify pid signal reason = 
               mapM_ (\(localpid,x) -> mapM_ (\(_,typ,how) -> sendNotification localpid pid typ how reason) (monitors x pid) ) (Map.toList (processRegistry global))
        doit = do q <- receiveWait [matchSignal,matchMonitor,matchMonitor2,matchUnknownThrow]
--                  liftIO $ print q
                  service q
      in doit >> return ()

----------------------------------------------
-- * Local registry
----------------------------------------------

localRegistryMagicProcess :: LocalProcessId
localRegistryMagicProcess = 38813

localRegistryMagicMagic :: String
localRegistryMagicMagic = "__LocalRegistry"

localRegistryMagicRole :: String
localRegistryMagicRole = "__LocalRegistry"

type LocalProcessName = String

type PeerInfo = Map.Map String [NodeId]

data LocalNodeData = LocalNodeData {ldmRoles :: PeerInfo} 

type RegistryData = Map.Map String LocalNodeData

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

standaloneLocalRegistry :: String -> IO ()
standaloneLocalRegistry cfgn = do cfg <- readConfig True (Just cfgn)
                                  res <- startLocalRegistry cfg True
                                  liftIO $ putStrLn $ "Terminating standalone local registry: " ++ show res

localRegistryRegisterNode :: ProcessM ()
localRegistryRegisterNode = localRegistryRegisterNodeImpl LocalNodeRegister

localRegistryUnregisterNode :: ProcessM ()
localRegistryUnregisterNode = localRegistryRegisterNodeImpl LocalNodeUnregister

localRegistryRegisterNodeImpl :: (String -> String -> NodeId -> LocalProcessMessage) -> ProcessM ()
localRegistryRegisterNodeImpl cons = 
    do cfg <- getConfig
       lrpid <- localRegistryPid
       nid <- getSelfNode
       let regMsg = cons (cfgNetworkMagic cfg) (cfgRole cfg) nid
       res <- roundtripQuery PldAdmin lrpid regMsg
       case res of
          Left ts -> throw $ TransmitException ts
          Right LocalNodeResponseOK -> return ()
          Right (LocalNodeResponseError s) -> throw $ TransmitException $ QteOther s

localRegistryHello :: ProcessM ()
localRegistryHello = do lrpid <- localRegistryPid
                        -- wait five seconds
                        res <- ptimeout 5000000 $ roundtripQuery PldAdmin lrpid LocalNodeHello 
                        case res of
                           Just (Right LocalNodeHello) -> return ()
                           Just (Left n) -> throw $ ConfigException $ "Can't talk to local node registry: " ++ show n
                           _ -> throw $ ConfigException $ "No response from local node registry"

localRegistryQueryNodes :: NodeId -> ProcessM (Maybe PeerInfo)
localRegistryQueryNodes nid = 
    do cfg <- getConfig
       lrpid <- remoteRegistryPid nid
       let regMsg = LocalNodeQuery (cfgNetworkMagic cfg)
       res <- roundtripQuery PldAdmin lrpid regMsg
       case res of
         Left ts -> return Nothing
         Right (LocalNodeAnswer pi) -> return $ Just pi

-- TODO since local registries are potentially sticky, there is good reason
-- to ensure that they don't get corrupted; there should be some
-- kind of security precaution here, e.g. making sure that registrations
-- only come from local nodes
startLocalRegistry :: Config -> Bool -> IO TransmitStatus
startLocalRegistry cfg waitforever = startit
 where
  regConfig = cfg {cfgListenPort = cfgLocalRegistryListenPort cfg, cfgNetworkMagic=localRegistryMagicMagic, cfgRole = localRegistryMagicRole}
  handler tbl = receiveWait [roundtripResponse PldAdmin (registryCommand tbl)] >>= handler
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
  startit = do node <- initNode regConfig Remote.Call.empty
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


