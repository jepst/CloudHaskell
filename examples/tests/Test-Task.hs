{-# LANGUAGE TemplateHaskell #-}
module Main where

-- A demonstration of the data dependency resolution 
-- of the Task layer. Each newPromise spawns a task
-- that calculates a value. Calls to adder and diver
-- take integers, but calls to merger take Promises,
-- which are an expression of the value computed
-- or yet-to-be-computed by another task. The
-- call to readPromise will retrieve that value
-- or wait until it's available.

-- You can run this program on one node, or
-- on several; the task layer will automatically
-- allocate tasks to whatever nodes it can talk to.
-- Unlike the Process layer, the programmer doesn't
-- need to specify the location.

import Remote

adder :: Int -> Int -> TaskM Int
adder a b = do return $ a + b

diver :: Int -> Int -> TaskM Int
diver a b = do return $ a `div` b

merger :: Promise Int -> Promise Int -> TaskM Int
merger a b = do a1 <- readPromise a
                b1 <- readPromise b
                return $ a1+b1


$( remotable ['diver,'adder, 'merger] )

initialProcess "MASTER" = 
                   do ans <- runTask $
                          do 
                             a <- newPromise (diver__closure 300 9)
                             b <- newPromise (diver__closure 300 5)
                             c <- newPromise (diver__closure 300 1)
                             d <- newPromise (adder__closure 300 9)
                             e <- newPromise (adder__closure 300 5)
                             f <- newPromise (adder__closure 300 1)
                             g <- newPromise (merger__closure a b)
                             h <- newPromise (merger__closure g c)
                             i <- newPromise (merger__closure c d)
                             j <- newPromise (merger__closure e f)
                             k <- newPromise (merger__closure i j)
                             l <- newPromise (merger__closure k g)
                             readPromise l
                      say $ show ans
initialProcess "WORKER" = receiveWait [] 

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

