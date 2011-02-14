module POTS.FakeTeleOS where

import Remote.Process
import Data.Char (isDigit)
import Control.Monad.Trans (liftIO)

type HardwareAddress = String  
notAnAddress :: HardwareAddress
notAnAddress = "null HardwareAddress"

data ToneType = DialTone | RingTone | BusyTone | FaultTone deriving (Show)

say :: String -> ProcessM ()
say = liftIO . putStrLn 

startRing :: HardwareAddress -> ProcessM ()
startRing ha = say $ concat ["Start ringing ", ha]

stopRing :: HardwareAddress -> ProcessM ()
stopRing ha = say $ concat ["Stop ringing ", ha]

connect :: HardwareAddress -> HardwareAddress -> ProcessM ()
connect ha1 ha2 = say $ concat ["Connection established between ", ha1," and ", ha2]

disconnect :: HardwareAddress -> HardwareAddress -> ProcessM ()
disconnect ha1 ha2 = say $ concat ["Connection broken between ", ha1," and ", ha2]

startTone :: HardwareAddress -> ToneType -> ProcessM ()
startTone ha tn = say $ concat ["Started ", show tn, " on ", ha]

stopTone :: HardwareAddress -> ProcessM ()
stopTone ha = say $ concat ["Stopped tone on ", ha]

data AnalyseResult = ArGetMoreDigits | ArInvalid | ArOK ProcessId HardwareAddress

analyse :: String -> ProcessM AnalyseResult
analyse s | not $ all isDigit s = return ArInvalid
analyse s | length s < 6 = return ArGetMoreDigits
analyse s = do res <- lookupProcessName s
               if res == nullPid 
                  then return ArInvalid
                  else return $ ArOK res s

