{-# LANGUAGE DeriveDataTypeable #-}
module KMeansCommon where

import Data.List (foldl')
import Data.Typeable (Typeable)
import Data.Data (Data)
import Remote.Encoding
import Data.Binary

data Vector = Vector Double Double deriving (Show,Read,Typeable,Data,Eq)
instance Binary Vector where put (Vector a b) = put a>>put b
                             get = do a<-get
                                      b<-get
                                      return $ Vector a b

data Cluster = Cluster
               {
                  clId :: Int,
                  clCount :: Int,
                  clSum :: Vector
               } deriving (Show,Read,Typeable,Data,Eq)
instance Binary Cluster where put (Cluster a b c) = put a>>put b>>put c
                              get = do a<-get
                                       b<-get
                                       c<-get
                                       return $ Cluster a b c

clusterCenter :: Cluster -> Vector
clusterCenter cl = let (Vector a b) = clSum cl
                    in Vector (op a) (op b)
       where op = (\d -> d / (fromIntegral $ clCount cl)) 

sqDistance :: Vector -> Vector -> Double
sqDistance (Vector x1 y1) (Vector x2 y2) = (x1-x2)*(x1-x2) + (y1-y2)*(y1-y2)


makeCluster :: Int -> [Vector] -> Cluster
makeCluster clid vecs = Cluster {clId = clid, clCount = length vecs, clSum = vecsum}
   where vecsum = foldl' addVector zeroVector vecs

addVector (Vector a b) (Vector c d) = Vector (a+c) (b+d)
zeroVector = Vector 0 0

getPoints :: FilePath -> IO [Vector]
getPoints fp = do c <- readFile fp
                  return $ read c

getClusters :: FilePath -> IO [Cluster]
getClusters fp = do c <- readFile fp
                    return $ read c


