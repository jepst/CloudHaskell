{-# LANGUAGE TemplateHaskell #-}
module Main where

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
                   do mypid <- getSelfPid
                      ans <- runTask $
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

