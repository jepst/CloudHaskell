{-# LANGUAGE TemplateHaskell #-}
module Main where

-- A simple word frequency counter using the task layer's mapreduce.

import Remote

import Control.Monad.Trans
import Control.Monad

import Debug.Trace

type Line = String
type Word = String

mrMapper :: [Line] -> TaskM [(Word, Int)]
mrMapper lines = 
      return $ concat $ map (\line -> map (\w -> (w,1)) (words line)) lines

mrReducer :: (Word,[Int]) -> TaskM (Word,Int)
mrReducer (w,p) = 
    return (w,sum p)

$( remotable ['mrMapper,  'mrReducer] )

myMapReduce = MapReduce 
               {
                 mtMapper = mrMapper__closure,
                 mtReducer = mrReducer__closure,
                 mtChunkify = chunkify 5,
                 mtShuffle = shuffle
               }

initialProcess "MASTER" = 
                   do args <- getCfgArgs
                      case args of
                        [filename] -> 
                          do file <- liftIO $ readFile filename
                             ans <- runTask $
                                do mapReduce myMapReduce (lines file)
                             say $ show ans
                        _ -> say "When starting MASTER, please also provide a filename on the command line"
initialProcess "WORKER" = receiveWait [] 
initialProcess _ = say "You need to start this program as either a MASTER or a WORKER. Set the appropiate value of cfgRole on the command line or in the config file."

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

