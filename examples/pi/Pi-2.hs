{-# LANGUAGE TemplateHaskell,BangPatterns #-}
module Main where

-- My modules for distributed computing
import Remote.Call
import Remote.Peer
import Remote.Process
import Remote.Init

-- Standard Haskell modules
import Data.Ratio
import Control.Monad
import Data.List

-- * THE MATHS PART OF THE PROGRAM

-- This is a hideously naive implementation of the Halton sequence
-- that gives appropriately hideous performance. I couldn't figure
-- out a reasonable way to translate the highly optimized Java
-- code from the Skywriting example into a functional version.
-- (I did try a memoized version and a literal translation of the Java 
-- code into Haskell using mutable arrays, which is somewhat faster but much
--  less clear.)
halton :: Integer -> Integer -> Ratio Integer
halton i base = h i (1/fromIntegral base)
  where
    h :: Integer -> Ratio Integer -> Ratio Integer 
    h 0 _ = 0
    h i half = let digit = i `mod` base
               in (h (floor(fromIntegral(i-digit)/fromIntegral base)) (half/fromIntegral base)) + fromIntegral digit*half
        
-- Gives a pair of coordinates based on Halton in base 2 and 3
haltonPair :: Integer -> (Ratio Integer,Ratio Integer)
haltonPair i = (halton i 2,halton i 3)

-- Given a starting index and a count, returns the number of values 
-- in that area of the Halton sequence that fall within and without
-- the unit circle. (Note that because of my bad implementation
-- of the Halton sequence, we end up computing the whole sequence
-- from the beginning even if we only use higher indices. Result: slow.)
getSums count offset = let outof = foldl' (\count coord -> count + if outCircle coord then 1 else 0) 0 sequence
                           inof = count-outof
                        in (inof,outof)
           where sequence = [ haltonPair i | i <- [offset..count+offset] ]
                 outCircle (x,y) = let fx=x-0.5 
                                       fy=y-0.5
                                   in fx*fx + fy*fy > 0.25

-- We do computation using ratios of arbitrary-precision integers. Here
-- we divide them and get output of a big string as a result.
longdiv :: Integer -> Integer -> Integer -> String
longdiv _ 0 _ = "<inf>"
longdiv numer denom places = let attempt = numer `div` denom in
                                if places==0
                                   then "" 
                                   else shows attempt (longdiv2 (numer - attempt*denom) denom (places -1))
     where longdiv2 numer denom places | numer `rem` denom == 0 = "0"
                                       | otherwise = longdiv (numer * 10) denom places

-- * THE INTERESTING PART OF THE PROGRAM

-- Code that will be called by a closure on a remote system needs to be enclosed
-- in a remoteCall block to generate appropriate metadata. In particular, this
-- block will, in addition to the mapper function below, generate a mapper__closure
-- function which returns a closure of a call to mapper. In both cases these
-- functions take a count and index of a Halton sequence to analyze, and a PID
-- of a Haskell process to send the results to. In the code below, the 'send'
-- function is the main message-sending primitive of my library.
$(remoteCall [d|
        mapper :: Integer -> Integer -> ProcessId -> ProcessM ()
        mapper count offset master = let (numin,numout) = getSums count offset
                                     in send master (numin::Integer,numout::Integer)
             |])
                                      
-- A "node" in this system probably corresponds to a computer. Each node
-- is assigned a role (by its config file) and based on its role will take
-- different action at startup. In this program, we distinguish two roles:
-- multiple MAPPER nodes, and a single REDUCER node. Here, we show the
-- initial action of MAPPER nodes, which is to do nothing at all until
-- they are told otherwise.
initialProcess "MAPPER" = do
           receiveWait []

-- And here is the code for the REDUCER node. Basically: it find all nearby
-- MAPPER nodes, and gives them each a chunk of the Halton sequence to look,
-- then waits for them to send back their responses, adds up the results,
-- prints an approximation of pi, and ends.
initialProcess "REDUCER" = do

           -- This gives us a list of all nearby MAPPER nodes, discovered
           -- with a UDP broadcast.
           peers <- queryDiscovery 50000
           let slaves = getPeerByRole peers "MAPPER"

           -- Here, interval is the number of number of samples to be
           -- processed by each mapper node.
           let interval = 10000

           -- mypid is now the process identifier of this, the main
           -- process running on the REDUCER node. A PID contains the
           -- the name of the host the process is running on and so
           -- should uniquely identify a thread in a network of
           -- multiple nodes.
           mypid <- getSelfPid
           say "Starting..."

           -- On each slave, spawn a new process that invoke the mapper function
           -- (given above), giving it as arguments the starting index and number
           -- of samples to process, as well as the process ID of the REDUCER process.
           -- 'spawnRemote' is my library's way of starting a new process on a node,
           -- and mapper__closure is the automagically generated closure function
           -- for mapper.
           mapM_ (\(offset,nid) -> 
                   do say $ "Telling slave " ++ show nid ++ " to look at range " ++ show offset ++ ".." ++ show (offset+interval-1)
                      spawnRemote nid (mapper__closure (interval-1) offset mypid)) (zip [0,interval..] slaves)

           -- Wait for all the MAPPERs to respond.
           (x,y) <- receiveLoop (0,0) (length slaves)

           -- Given the number of samples in the unit circle and number out,
           -- estimate pi.
           let est = estimatePi x y
           say $ "Done: " ++ longdiv (numerator est) (denominator est) 100
      where estimatePi ni no | ni+no==0 = 0
                             | otherwise = (4 * ni) % (ni+no)
            receiveLoop a 0 = return a
            receiveLoop (numIn,numOut) n = 
                  let 
                     resultMatch = match (\(x,y) -> return (x::Integer,y::Integer))
                  in do (newin,newout) <- receiveWait [resultMatch]
                        -- receiveWait is my library's primitive function to wait
                        -- for a message of a particular type. In this case, we're
                        -- waiting for a tuple of integers sent by mapper, containing the
                        -- the number of points in a circle and out of it.
                        let x = numIn + newin
                        let y = numOut + newout
                        receiveLoop (x,y) (n-1)

initialProcess _ = error "Role must be MAPPER or REDUCER"

-- Entry point. Reads config file, starts node, invokes initialProcess (given above).
main = remoteInit "config" [Main.__remoteCallMetaData] initialProcess


{- Sample run, with three MAPPERs:

2011-01-27 19:00:36.324802 UTC 0 pid://velikan:58565/6/       SAY Starting...
2011-01-27 19:00:36.325401 UTC 0 pid://velikan:58565/6/       SAY Telling slave nid://velikan:48599/ to look at range 0..9999
2011-01-27 19:00:36.327816 UTC 0 pid://velikan:58565/6/       SAY Telling slave nid://velikan:52089/ to look at range 10000..19999
2011-01-27 19:00:36.3301 UTC 0 pid://velikan:58565/6/       SAY Telling slave nid://velikan:37615/ to look at range 20000..29999
2011-01-27 19:00:37.595462 UTC 0 pid://velikan:58565/6/       SAY Done: 3141380804747141380804747141380804747141380804747141380804747141

-}
