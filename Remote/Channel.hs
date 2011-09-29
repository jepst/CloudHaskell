{-# LANGUAGE ExistentialQuantification,DeriveDataTypeable #-}

-- | This module provides typed channels, an alternative
-- approach to interprocess messaging. Typed channels
-- can be used in combination with or instead of the
-- the untyped channels available in the "Remote.Process"
-- module via 'send'.
module Remote.Channel (
                       -- * Basic typed channels
                       SendPort,ReceivePort,newChannel,sendChannel,receiveChannel,

                       -- * Combined typed channels
                       CombinedChannelAction,combinedChannelAction,
                       combinePortsBiased,combinePortsRR,mergePortsBiased,mergePortsRR,

                       -- * Terminate a channel
                       terminateChannel) where

import Remote.Process (ProcessM,send,getMessageType,getMessagePayload,setDaemonic,getProcess,prNodeRef,getNewMessageLocal,localFromPid,isPidLocal,TransmitException(..),TransmitStatus(..),spawnLocalAnd,ProcessId,Node,UnknownMessageException(..))
import Remote.Encoding (Serializable)

import Data.List (foldl')
import Data.Binary (Binary,get,put)
import Data.Typeable (Typeable)
import Control.Exception (throw)
import Control.Monad (when)
import Control.Monad.Trans (liftIO)
import Control.Concurrent.MVar (MVar,newEmptyMVar,takeMVar,readMVar,putMVar)
import Control.Concurrent.STM (STM,atomically,retry,orElse)
import Control.Concurrent.STM.TVar (TVar,newTVarIO,readTVar,writeTVar)

----------------------------------------------
-- * Channels
----------------------------------------------

-- | A channel is a unidirectional communication pipeline
-- with two ends: a sending port, and a receiving port.
-- This is the sending port. A process holding this
-- value can insert messages into the channel. SendPorts
-- themselves can also be sent to other processes.
-- The other side of the channel is the 'ReceivePort'.
newtype SendPort a = SendPort ProcessId deriving (Typeable)

-- | A process holding a ReceivePort can extract messages
-- from the channel, which we inserted by
-- the holder(s) of the corresponding 'SendPort'.
-- Critically, ReceivePorts, unlike SendPorts, are not serializable.
-- This means that you can only receive messages through a channel
-- on the node on which the channel was created.
data ReceivePort a = ReceivePortSimple ProcessId (MVar ())
                      | ReceivePortBiased [Node -> STM a]
                      | ReceivePortRR (TVar [Node -> STM a])

instance Binary (SendPort a) where
   put (SendPort pid) = put pid
   get = get >>= return . SendPort

-- | Create a new channel, and returns both the 'SendPort'
-- and 'ReceivePort' thereof.
newChannel :: (Serializable a) => ProcessM (SendPort a, ReceivePort a)
newChannel = do mv <- liftIO $ newEmptyMVar
                pid <- spawnLocalAnd (body mv) setDaemonic
                return (SendPort pid,
                       ReceivePortSimple pid mv)
     where body mv = liftIO (takeMVar mv)

-- | Inserts a new value into the channel.
sendChannel :: (Serializable a) => SendPort a -> a -> ProcessM ()
sendChannel (SendPort pid) a = send pid a

-- | Extract a value from the channel, in FIFO order.
receiveChannel :: (Serializable a) => ReceivePort a -> ProcessM a
receiveChannel rc = do p <- getProcess
                       channelCheckPids [rc]
                       node <- liftIO $ readMVar (prNodeRef p)
                       liftIO $ atomically $ receiveChannelImpl node rc

receiveChannelImpl :: (Serializable a) => Node -> ReceivePort a -> STM a
receiveChannelImpl node rc =
                       case rc of
                         ReceivePortBiased l -> foldl' orElse retry (map (\x -> x node) l)
                         ReceivePortRR mv -> do tv <- readTVar mv
                                                writeTVar mv (rotate tv)
                                                foldl' orElse retry (map (\x -> x node) tv)
                         ReceivePortSimple _ _ -> receiveChannelSimple node rc
      where rotate [] = []
            rotate (h:t) = t ++ [h]

data CombinedChannelAction b = forall a. (Serializable a) => CombinedChannelAction (ReceivePort a) (a -> b)

-- | Specifies a port and an adapter for combining ports via 'combinePortsBiased' and
-- 'combinePortsRR'.
combinedChannelAction :: (Serializable a) => ReceivePort a -> (a -> b) -> CombinedChannelAction b
combinedChannelAction = CombinedChannelAction

-- | This function lets us respond to messages on multiple channels
-- by combining several 'ReceivePort's into one. The resulting port
-- is the sum of the input ports, and will extract messages from all
-- of them in FIFO order. The input ports are specified by 
-- 'combinedChannelAction', which also gives a converter function.
-- After combining the underlying receive ports can still
-- be used independently, as well.
-- We provide two ways to combine ports, which differ bias
-- they demonstrate in returning messages when more than one
-- underlying channel is nonempty. combinePortsBiased will
-- check ports in the order given by its argument, and so
-- if the first channel always was a message waiting, it will.
-- starve the other channels. The alternative is 'combinePortsRR'.
combinePortsBiased :: Serializable b => [CombinedChannelAction b] -> ProcessM (ReceivePort b)
combinePortsBiased chns = do mapM_ (\(CombinedChannelAction chn _ ) -> channelCheckPids [chn]) chns
                             return $ ReceivePortBiased [(\node -> receiveChannelImpl node chn >>= return . fun) | (CombinedChannelAction chn fun) <- chns]

-- | See 'combinePortsBiased'. This function differs from that one
-- in that the order that the underlying ports are checked is rotated
-- with each invocation, guaranteeing that, given enough invocations,
-- every channel will have a chance to contribute a message.
combinePortsRR :: Serializable b => [CombinedChannelAction b] -> ProcessM (ReceivePort b)
combinePortsRR chns = do mapM_ (\(CombinedChannelAction chn _ ) -> channelCheckPids [chn]) chns
                         tv <- liftIO $ newTVarIO [(\node -> receiveChannelImpl node chn >>= return . fun) | (CombinedChannelAction chn fun) <- chns]
                         return $ ReceivePortRR tv

-- | Similar to 'combinePortsBiased', with the difference that the
-- the underlying ports must be of the same type, and you don't
-- have the opportunity to provide an adapter function.
mergePortsBiased :: (Serializable a) => [ReceivePort a] -> ProcessM (ReceivePort a)
mergePortsBiased chns = do channelCheckPids chns
                           return $ ReceivePortBiased [(\node -> receiveChannelImpl node chn) | chn <- chns]

-- | Similar to 'combinePortsRR', with the difference that the
-- the underlying ports must be of the same type, and you don't
-- have the opportunity to provide an adapter function.
mergePortsRR :: (Serializable a) => [ReceivePort a] -> ProcessM (ReceivePort a)
mergePortsRR chns = do channelCheckPids chns
                       tv <- liftIO $ newTVarIO [(\node -> receiveChannelImpl node chn) | chn <- chns]
                       return $ ReceivePortRR tv

channelCheckPids :: (Serializable a) => [ReceivePort a] -> ProcessM ()
channelCheckPids chns = mapM_ checkPid chns
         where checkPid (ReceivePortSimple pid _) = do islocal <- isPidLocal pid
                                                       when (not islocal)
                                                          (throw $ TransmitException QteUnknownPid)
               checkPid _ = return ()

receiveChannelSimple :: (Serializable a) => Node -> ReceivePort a -> STM a
receiveChannelSimple node (ReceivePortSimple chpid _) = 
             do mmsg <- getNewMessageLocal (node) (localFromPid chpid)
                case mmsg of
                   Nothing -> badPid
                   Just msg -> case getMessagePayload msg of
                                    Nothing -> throw $ UnknownMessageException (getMessageType msg)
                                    Just q -> return q
   where badPid = throw $ TransmitException QteUnknownPid

-- | Terminate a channel. After calling this function, 'receiveChannel'
-- on that port (or on any combined port based on it) will either 
-- fail or block indefinitely, and 'sendChannel' on the corresponding
-- 'SendPort' will fail. Any unread messages remaining in the channel
-- will be lost.
terminateChannel :: (Serializable a) => ReceivePort a -> ProcessM ()
terminateChannel (ReceivePortSimple _ term) = liftIO $ putMVar (term) ()
terminateChannel _ = throw $ TransmitException QteUnknownPid
