{-# LANGUAGE TemplateHaskell #-}
module Main where	

-- This file contains some examples of closures, which are the
-- way to express a function invocation in Cloud Haskell. You
-- need to use closures to run code on a remote system, which is,
-- after all, the whole point. See the documentation on the function
-- 'remotable' for more details. Understanding this example is
-- essential to effectively using Cloud Haskell.

import Remote
import Remote.Call (mkClosure)

import Data.List (sort)

-- Step #1: Define the functions that we would
-- like to call remotely. These are just regular
-- functions.
           
sayHi :: String -> ProcessM ()
sayHi s = say $ "Greetings, " ++ s

sayHiPM :: String -> ProcessM String
sayHiPM s = say ("Hello, "++s) >> 
               return (sort s)

sayHiIO :: String -> IO String
sayHiIO s = putStrLn ("Hello, " ++ s) >>
                return (reverse s)

sayHiPure :: String -> String
sayHiPure s = "Hello, " ++ s

simpleSum :: Int -> Int -> Int
simpleSum a b = a + b + 1

-- You can also manually call a closure using invokeClosure.
-- This is what functions like spawn and callRemote do
-- internally.
runAnother :: Closure Int -> ProcessM String
runAnother c = do mi <- invokeClosure c
                  case mi of
                    Just i -> return $ concat $ replicate i "Starscream"
                    Nothing -> return "No good"

-- This a partial closure: some parameters are provided by the caller,
-- and some are provided after the closure is invoked. You have to
-- write the function in a funny way to make this work automatically,
-- but otherwise it's pretty straightforward.
funnyHi :: String -> ProcessM (Int -> ProcessM ())
funnyHi s = return $ \i -> say ("Hello, " ++ (concat $ replicate i (reverse s)))

-- Step #2: Automagically generate closures
-- for these functions using remotable. For each
-- given function n, remotable will create a
-- closure for that function named n__closure.
-- You can then use that closure with spawn, remoteCall,
-- and invokeClosure. See examples below.

remotable ['sayHi, 'sayHiIO,'sayHiPure, 'sayHiPM, 'funnyHi, 'runAnother, 'simpleSum]

initialProcess _ = do
              mynid <- getSelfNode

              -- spawn and callRemote (and their variants) run
              -- a function on a given node. We indicate which
              -- node by giving a node ID (to keep it simple
              -- we do everything on one node in this
              -- example), and we indicate which function to run
              -- by providing its closure.

              -- A simple spawn. Does not block, and the result
              -- we get back is the PID of the new process.
              p <- spawn mynid (sayHi__closure "Zoltan")
              say $ "Got result " ++ show p

              -- callRemote is like a synchronous version of spawn.
              -- It will block until the function ends, and returns
              -- its result.
              v <- callRemote mynid ( sayHiPM__closure "Jaroslav")
              say $ "Got result " ++ v

              -- We need a different function to call closures in the
              -- IO monad. Also, instead of using the "something__closure"
              -- syntax, you can call the Template Haskell mkClosure
              -- function, which expands to the same thing.
              w <- callRemoteIO mynid ( $(mkClosure 'sayHiIO) "Noodle")
              say $ "Got result " ++ show w

              -- Yet another version of callRemote for nonmonadic functions.
              q <- callRemotePure mynid (sayHiPure__closure "Spatula")
              say $ "Got result " ++ show q

              -- We can even give closures to closures. They can in turn run
              -- them indirectly (with spawn or callRemote) or directly
              -- (with invokeClosure).
              x <- callRemote mynid (runAnother__closure (simpleSum__closure 1 1))
              say $ "Got result " ++ x

              -- This function takes some parameters after closure invocation.
              -- So the value we get back from invokeClosure is actually
              -- a partially evaluated function.
              mfunnyFun <- invokeClosure (funnyHi__closure "Antwerp")
              case mfunnyFun of
                Just funnyFun -> funnyFun 3
                Nothing -> say "No good"

              return ()

main = remoteInit (Just "config") [Main.__remoteCallMetaData] initialProcess

