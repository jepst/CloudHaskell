{-# LANGUAGE  DeriveDataTypeable #-}

-- | Exposes mechanisms for a program built on the "Remote.Process"
-- framework to discover nodes on the current network. Programs
-- can perform node discovery manually, or they can use "Remote.Task",
-- which does it automatically.
module Remote.Peer (PeerInfo,startDiscoveryService,getPeers,getPeersStatic,getPeersDynamic,findPeerByRole) where

import Prelude hiding (all, pi)

import Network.Socket (defaultHints,sendTo,recv,sClose,Socket,getAddrInfo,AddrInfoFlag(..),setSocketOption,addrFlags,addrSocketType,addrFamily,SocketType(..),Family(..),addrProtocol,SocketOption(..),AddrInfo,bindSocket,addrAddress,SockAddr(..),socket)
import Network.BSD (getProtocolNumber)
import qualified Control.Exception.Lifted as Lifted (try)
import qualified System.Timeout.Lifted as Lifted (timeout)
import Control.Concurrent.MVar (takeMVar, newMVar, modifyMVar_)
import Remote.Process (PeerInfo,pingNode,makeNodeFromHost,spawnLocalAnd,setDaemonic,TransmitStatus(..),TransmitException(..),PayloadDisposition(..),getSelfNode,sendSimple,cfgRole,cfgKnownHosts,cfgPeerDiscoveryPort,match,receiveWait,getSelfPid,getConfig,NodeId,PortId,ProcessM,localRegistryQueryNodes)
import Control.Monad.Trans (liftIO)
import Data.Typeable (Typeable)
import Data.Maybe (catMaybes)
import Data.Binary (Binary,get,put)
import Control.Exception (try,bracket,ErrorCall(..),throw)
import Data.List (nub)
import Control.Monad (filterM)
import qualified Data.Traversable as Traversable (mapM) 
import qualified Data.Map as Map (unionsWith,insertWith,empty,lookup)

data DiscoveryInfo = DiscoveryInfo
     {
       discNodeId :: NodeId,
       discRole :: String
     } deriving (Typeable,Eq)

instance Binary DiscoveryInfo where
   put (DiscoveryInfo nid role) = put nid >> put role
   get = get >>= \nid -> get >>= \role -> return $ DiscoveryInfo nid role

getUdpSocket :: PortId -> IO (Socket,AddrInfo) -- mostly copied from Network.Socket
getUdpSocket port = do
    proto <- getProtocolNumber "udp"
    let hints = defaultHints { addrFlags = [AI_PASSIVE,AI_ADDRCONFIG]
                             , addrSocketType = Datagram
                             , addrFamily = AF_INET -- only INET supports broadcast
                             , addrProtocol = proto }
    addrs <- getAddrInfo (Just hints) Nothing (Just (show port))
    let addr = head addrs
    s <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    return (s,addr)

maxPacket :: Int
maxPacket = 1024

listenUdp :: PortId -> IO String
listenUdp port = 
    bracket
        (getUdpSocket port)
	(\(s,_) -> sClose s)
	(\(sock,addr) -> do
	    setSocketOption sock ReuseAddr 1
	    bindSocket sock (addrAddress addr)
	    msg <- recv sock maxPacket
            return msg
	)

sendBroadcast :: PortId -> String -> IO ()
sendBroadcast port str 
    | length str > maxPacket = throw $ TransmitException $ QteOther $ "sendBroadcast: Specified packet is too big for UDP broadcast, having a length of " ++ (show $ length str)
    | otherwise = bracket
        (getUdpSocket port >>= return . fst)
	(sClose)
	(\sock -> do
            setSocketOption sock Broadcast 1
	    _res <- sendTo sock str (SockAddrInet (toEnum port) (-1))
            return ()
	)

-- | Returns information about all nodes on the current network
-- that this node knows about. This function combines dynamic
-- and static mechanisms. See documentation on 'getPeersStatic' 
-- and 'getPeersDynamic' for more info. This function depends
-- on the configuration values @cfgKnownHosts@ and @cfgPeerDiscoveryPort@.
getPeers :: ProcessM PeerInfo
getPeers = do a <- getPeersStatic
              b <- getPeersDynamic 500000
              verifyPeerInfo $ Map.unionsWith (\x y -> nub $ x ++ y) [a,b]

verifyPeerInfo :: PeerInfo -> ProcessM PeerInfo
verifyPeerInfo pi = Traversable.mapM verify1 pi
        where verify1 = filterM pingNode -- TODO ping should require a response

-- | Returns a PeerInfo, containing a list of known nodes ordered by role.
-- This information is acquired by querying the local node registry on
-- each of the hosts in the cfgKnownHosts entry in this node's config.
-- Hostnames that don't respond are assumed to be down and nodes running
-- on them won't be included in the results.
getPeersStatic :: ProcessM PeerInfo
getPeersStatic = do cfg <- getConfig
                    let peers = cfgKnownHosts cfg
                    peerinfos <- mapM (localRegistryQueryNodes . hostToNodeId) peers
                    return $ Map.unionsWith (\a b -> nub $ a ++ b) (catMaybes peerinfos)
     where hostToNodeId host = makeNodeFromHost host 0

-- | Returns a PeerInfo, containing a list of known nodes ordered by role.
-- This information is acquired by sending out a UDP broadcast on the
-- local network; active nodes running the discovery service 
-- should respond with their information.
-- If nodes are running outside of the local network, or if UDP broadcasts
-- are disabled by firewall configuration, this won't return useful
-- information; in that case, use getPeersStatic.
-- This function takes a parameter indicating how long in microseconds 
-- to wait for hosts to respond. A number like 50000 is usually good enough,
-- unless your network is highly congested or with high latency.
getPeersDynamic :: Int -> ProcessM PeerInfo
getPeersDynamic t = 
   do pid <- getSelfPid
      cfg <- getConfig
      case (cfgPeerDiscoveryPort cfg) of
          0 -> return Map.empty
          port -> do  -- TODO should send broacast multiple times in case of packet loss
                      _ <- liftIO $ try $ sendBroadcast port (show pid) :: ProcessM (Either IOError ())
                      responses <- liftIO $ newMVar []
                      _ <- Lifted.timeout t (receiveInfo responses)
                      res <- liftIO $ takeMVar responses
                      let all = map (\di -> (discRole di,[discNodeId di])) (nub res)
                      return $ foldl (\a (k,v) -> Map.insertWith (++) k v a ) Map.empty all
     where receiveInfo responses = let matchInfo = match (\x -> liftIO $ modifyMVar_ responses (\m -> return (x:m))) in
                         receiveWait [matchInfo] >> receiveInfo responses

-- | Given a PeerInfo returned by getPeersDynamic or getPeersStatic,
-- give a list of nodes registered as a particular role. If no nodes of
-- that role are found, the empty list is returned.
findPeerByRole :: PeerInfo -> String -> [NodeId]
findPeerByRole disc role = maybe [] id (Map.lookup role disc)

{- UNUSED
findRoles :: PeerInfo -> [String]
findRoles disc = Map.keys disc
-}

waitForDiscovery :: Int -> ProcessM Bool
waitForDiscovery delay 
              | delay <= 0 = doit 
              | otherwise = Lifted.timeout delay doit >>= (return . maybe False id)
           where doit =
                   do cfg <- getConfig
                      msg <- liftIO $ listenUdp (cfgPeerDiscoveryPort cfg)
                      nodeid <- getSelfNode
                      res <- Lifted.try $ sendSimple (read msg) (DiscoveryInfo {discNodeId=nodeid,discRole=cfgRole cfg}) PldUser
                                 :: ProcessM (Either ErrorCall TransmitStatus)
                      case res of
                         Right QteOK -> return True
                         _ -> return False

-- | Starts the discovery process, allowing this node to respond to
-- queries from getPeersDynamic. You don't want to call this yourself,
-- as it's called for you in 'Remote.Init.remoteInit'
startDiscoveryService :: ProcessM ()
startDiscoveryService = 
   do cfg <- getConfig
      if cfgPeerDiscoveryPort cfg /= 0
         then spawnLocalAnd service setDaemonic >> return ()
         else return ()
  where service = waitForDiscovery 0 >> service
