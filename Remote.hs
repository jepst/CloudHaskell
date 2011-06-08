-- | Cloud Haskell (previously Remote Haskell) is a distributed computing
-- framework for Haskell. We can describe its interface
-- as roughly two levels: the /process layer/, consisting of
-- processes, messages, and fault monitoring; and the
-- /task layer/, consisting of tasks, promises, and fault recovery.
-- This summary module provides the most common interface
-- functions for both layers, although advanced users might want to import names
-- from the other constituent modules, as well.

module Remote ( -- * The process layer
                remoteInit,

                ProcessM, NodeId, ProcessId, MatchM,
                getSelfPid, getSelfNode,

                send,sendQuiet,

                spawn, spawnLocal, spawnAnd,
                spawnLink,
                callRemote, callRemotePure, callRemoteIO,
                AmSpawnOptions(..), defaultSpawnOptions,
                terminate,

                expect, receive, receiveWait, receiveTimeout,
                match, matchIf, matchUnknown, matchUnknownThrow, matchProcessDown,

                logS, say, LogSphere(..), LogTarget(..), LogFilter(..), LogConfig(..), LogLevel(..),
                setLogConfig, setNodeLogConfig, getLogConfig, defaultLogConfig, getCfgArgs,

                UnknownMessageException(..), ServiceException(..), 
                TransmitException(..), TransmitStatus(..),

                nameSet, nameQuery, nameQueryOrStart,

                linkProcess, monitorProcess, unmonitorProcess,
                withMonitor, MonitorAction(..),
                ProcessMonitorException(..),

                getPeers, findPeerByRole, PeerInfo,

                remotable, RemoteCallMetaData, Lookup,

                Closure, makeClosure, invokeClosure,
                Payload, genericPut, genericGet, Serializable,

                -- * Channels

                SendPort, ReceivePort,
                newChannel, sendChannel, receiveChannel,
                
                CombinedChannelAction, combinedChannelAction,
                combinePortsBiased, combinePortsRR, mergePortsBiased, mergePortsRR,
                terminateChannel,

                -- * The task layer

                TaskM, runTask, Promise,
                newPromise, newPromiseHere, newPromiseAtRole, newPromiseNear,
                toPromise, toPromiseNear, toPromiseImm,
                readPromise,
                tlogS, tsay,

                TaskException(..),

                MapReduce(..), mapReduce,
                chunkify, shuffle,

              ) where

import Remote.Init
import Remote.Encoding
import Remote.Process
import Remote.Channel
import Remote.Call
import Remote.Task 
import Remote.Peer
import Remote.Reg
import Remote.Closure
