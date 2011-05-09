{-# LANGUAGE TemplateHaskell #-}
module Main where

import Remote.Process
import Remote.Task
import Remote.Init
import Remote.Call

import Control.Monad.Trans
import Control.Monad

import Debug.Trace

type Line = String
type Word = String

mrMapper :: [Line] -> TaskM [(Word, Promise Int)]
mrMapper lines = 
    do one <- asPromise (1::Int)
       return $ concat $ map (\line -> map (\w -> (w,one)) (words line)) lines

mrReducer :: Word -> [Promise Int] -> TaskM (Word,Int)
mrReducer w p = 
  do m <- trace w $ mapM readPromise p
     return (w,sum m)

$( remotable ['mrMapper,  'mrReducer] )

myMapReduce = MapReduce 
               {
                 mtMapper = mrMapper__closure,
                 mtReducer = mrReducer__closure,
                 mtChunkify = chunkify 5
               }

initialProcess "MASTER" = 
                   do
                      file <- liftIO $ readFile "journal"
                      ans <- startMaster $
                        do 
                           res <- mapReduce myMapReduce (lines file)
                           return res
                      say $ show ans
initialProcess "WORKER" = receiveWait [] 

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

