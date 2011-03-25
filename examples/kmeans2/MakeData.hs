module Main where

import System.Random (randomR,getStdRandom)
import System.Environment
import KMeansCommon
import Data.Binary
import System.IO
import Data.Array.Unboxed
import qualified Data.ByteString.Lazy as B


-- vectorsPerFile = 80000 -- should be 80000
numClusters = 100
vectorDimensions = 100 -- should be 100
minValue = -1.0
maxValue = 1.0

val = getStdRandom (randomR (minValue,maxValue))
vector = do vals <- mapM (const val) [1..vectorDimensions]
            return $ Vector $ listArray (0,vectorDimensions-1) vals
file vectorsPerFile = mapM (const vector) [1..vectorsPerFile]

clusters = mapM (\x -> do v <- vector
                          return $ Cluster {clId = x,clCount=1,clSum=v}) [1..numClusters]
            

makeBig :: Int -> IO ()
makeBig i = do c <- clusters
               p <- file i
               withFile "kmeans-points" WriteMode (\h -> mapM (\v -> hPutStrLn h (show v)) p)
               writeFile "kmeans-clusters" $ show c

main = do a <- getArgs
          case a of
            ["big",a] -> makeBig (read a)
            _ -> putStrLn "Syntax:\n\tMakeData big 8\n\t\nOutput is in kmeans-points and kmeans-clusters"
