module Main where

import System.Random (randomR,getStdRandom)
import System.Environment
import KMeansCommon

makeData :: Int -> Int -> IO ()
makeData n k = 
           let getrand = getStdRandom (randomR (1,1000))
               randvect = do a <- getrand
                             b <- getrand
                             return $ Vector a b
               vects :: IO [Vector]
               vects = sequence (replicate n randvect)
               clusts :: IO [Cluster]
               clusts = mapM (\clid -> do v <- randvect
                                          return $ Cluster {clId = clid,clCount=1,clSum=v}) [1..k]
           in do v <- vects
                 c <- clusts
                 writeFile "kmeans-points" (show v)
                 writeFile "kmeans-clusters" (show c)

makeBlobs :: Int -> IO ()
makeBlobs n =
            let getrand a b = getStdRandom (randomR (a,b))
                randvectr (x,y) r = do rr <- (getrand 0 r)
                                       ra <- (getrand 0 (2*3.1415))
                                       let xo = cos ra * rr
                                       let yo = sin ra * rr
                                       return $ Vector  (x+xo) (y+yo)
                randvect (xmin,xmax) (ymin,ymax) = do x <- (getrand xmin xmax)
                                                      y <- (getrand ymin ymax)
                                                      return $ Vector  x y
                area count size (x,y) = sequence (replicate count (randvectr (x,y) size))
                clusts = mapM (\clid -> do v <- randvect (0,1000) (0,1000)
                                           return $ Cluster {clId = clid,clCount=1,clSum=v}) [1..n]
            in do areas <- mapM (\_ ->
                                 do count <- getrand 50 500
                                    size <- getrand 5 100
                                    x <- getrand 0 1000
                                    y <- getrand 0 1000
                                    area count size (x,y)) [1..n]
                  c <- clusts
                  writeFile "kmeans-points" (show (concat areas))
                  writeFile "kmeans-clusters" (show c)

makeMouse :: Int -> IO ()
makeMouse d = 
            let getrand a b = getStdRandom (randomR (a,b))
                randvectr (x,y) r = do rr <- (getrand 0 r)
                                       ra <- (getrand 0 (2*3.1415))
                                       let xo = cos ra * rr
                                       let yo = sin ra * rr
                                       return $ Vector  (x+xo) (y+yo)
                randvect (xmin,xmax) (ymin,ymax) = do x <- (getrand xmin xmax)
                                                      y <- (getrand ymin ymax)
                                                      return $ Vector  x y
                area count size (x,y) = sequence (replicate count (randvectr (x,y) size))
                clusts = mapM (\clid -> do v <- randvect (0,1000) (0,1000)
                                           return $ Cluster {clId = clid,clCount=1,clSum=v}) [1..3]
             in do ear1 <- area (2*d) 100 (200,200)
                   ear2 <- area (4*d) 100 (800,200)
                   face <- area (10*d) 350 (500,600)
                   c <- clusts
                   writeFile "kmeans-points" (show (ear1++ear2++face))
                   writeFile "kmeans-clusters" (show c)

main = do a <- getArgs
          case a of
            ["mouse",a] -> makeMouse (read a)
            ["blobs",n] -> makeBlobs (read n)
            ["rand",n,k] -> makeData (read n) (read k)
            _ -> putStrLn "Syntax:\n\tMakeData mouse m\n\tMakeData rand n k\n\tMakeData blobs n\nOutput is in kmeans-points and kmeans-clusters"
