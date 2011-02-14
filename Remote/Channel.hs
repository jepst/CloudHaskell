{-# LANGUAGE ExistentialQuantification,DeriveDataTypeable #-}
module Remote.Channel (SendPort,ReceivePort,makeChannel,sendChannel,receiveChannel,
                       CombinedChannelAction,combinedChannelAction,
                       combineChannelsBiased,combineChannelsRR,mergeChannelsBiased,mergeChannelsRR,
                       terminateChannel) where

import Remote.Process (ProcessM,send,setDaemonic,getProcess,prNodeRef,getNewMessageLocal,localFromPid,isPidLocal,pteChannel,TransmitException(..),TransmitStatus(..),Message,msgPayload,spawn,ProcessId,Node,UnknownMessageException(..))
import Remote.Encoding (getPayloadType,serialDecodePure,Serializable)

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

newtype SendPort a = SendPort ProcessId deriving (Typeable)
data ReceivePort a = ReceivePortSimple ProcessId (MVar ())
                      | ReceivePortBiased [Node -> STM a]
                      | ReceivePortRR (TVar [Node -> STM a])

instance Binary (SendPort a) where
   put (SendPort pid) = put pid
   get = get >>= return . SendPort

makeChannel :: (Serializable a) => ProcessM (SendPort a, ReceivePort a)
makeChannel = do mv <- liftIO $ newEmptyMVar
                 pid <- spawn (body mv)
                 return (SendPort pid,
                         ReceivePortSimple pid mv)
     where body mv = setDaemonic >> liftIO (takeMVar mv)

sendChannel :: (Serializable a) => SendPort a -> a -> ProcessM ()
sendChannel (SendPort pid) a = send pid a

receiveChannel :: (Serializable a) => ReceivePort a -> ProcessM a
receiveChannel rc = do p <- getProcess
                       node <- liftIO $ readMVar (prNodeRef p)
                       channelCheckPids [rc]
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

combinedChannelAction :: (Serializable a) => ReceivePort a -> (a -> b) -> CombinedChannelAction b
combinedChannelAction = CombinedChannelAction

combineChannelsBiased :: Serializable b => [CombinedChannelAction b] -> ProcessM (ReceivePort b)
combineChannelsBiased chns = do mapM_ (\(CombinedChannelAction chn _ ) -> channelCheckPids [chn]) chns
                                return $ ReceivePortBiased [(\node -> receiveChannelImpl node chn >>= return . fun) | (CombinedChannelAction chn fun) <- chns]

combineChannelsRR :: Serializable b => [CombinedChannelAction b] -> ProcessM (ReceivePort b)
combineChannelsRR chns = do mapM_ (\(CombinedChannelAction chn _ ) -> channelCheckPids [chn]) chns
                            tv <- liftIO $ newTVarIO [(\node -> receiveChannelImpl node chn >>= return . fun) | (CombinedChannelAction chn fun) <- chns]
                            return $ ReceivePortRR tv

mergeChannelsBiased :: (Serializable a) => [ReceivePort a] -> ProcessM (ReceivePort a)
mergeChannelsBiased chns = do channelCheckPids chns
                              return $ ReceivePortBiased [(\node -> receiveChannelImpl node chn) | chn <- chns]

mergeChannelsRR :: (Serializable a) => [ReceivePort a] -> ProcessM (ReceivePort a)
mergeChannelsRR chns = do channelCheckPids chns
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
                   Just msg -> case serialDecodePure (msgPayload msg) of
                                    Nothing -> throw $ UnknownMessageException (getPayloadType $ msgPayload msg)
                                    Just q -> return q
   where badPid = throw $ TransmitException QteUnknownPid

terminateChannel :: (Serializable a) => ReceivePort a -> ProcessM ()
terminateChannel (ReceivePortSimple _ term) = liftIO $ putMVar (term) ()
terminateChannel _ = throw $ TransmitException QteUnknownPid
