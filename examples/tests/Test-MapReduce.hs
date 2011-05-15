{-# LANGUAGE TemplateHaskell #-}
module Main where

-- A simple word frequency counter using the task layer's mapreduce.

import Remote

import Control.Monad.Trans
import Control.Monad

import Debug.Trace

type Line = String
type Word = String

mrMapper :: [Line] -> TaskM [(Word, Promise Int)]
mrMapper lines = 
    do one <- toPromiseImm (1::Int)
       return $ concat $ map (\line -> map (\w -> (w,one)) (words line)) lines

mrReducer :: Word -> [Promise Int] -> TaskM (Word,Int)
mrReducer w p = 
  do m <- mapM readPromise p
     return (w,sum m)

$( remotable ['mrMapper,  'mrReducer] )

myMapReduce = MapReduce 
               {
                 mtMapper = mrMapper__closure,
                 mtReducer = mrReducer__closure,
                 mtChunkify = chunkify 5
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

