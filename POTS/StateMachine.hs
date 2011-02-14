{-# LANGUAGE TemplateHaskell,DeriveDataTypeable,RankNTypes #-}

module POTS.StateMachine where

import POTS.FakeTeleOS

import Remote.Process
import Remote.Call (registerCalls,remoteCall)
import Remote.Encoding (genericGet, genericPut)

import Control.Monad (when)
import Control.Monad.Trans (liftIO)
import Control.Concurrent (threadDelay)

import Data.Data
import Data.Typeable
import Data.Binary

while :: (Monad m) => m Bool -> m ()
while a = do f <- a
             when (f)
                (while a >> return ())
             return ()



data TelephoneState = IsOnHook | IsOffHook

data TelephoneMessage = TmOffHook | TmOnHook | TmDigitDialed Char deriving (Data, Typeable, Eq)
instance Binary TelephoneMessage where
   get = genericGet
   put = genericPut
-- sent from the telephone hardware

isOffHookTm tm = tm == TmOffHook
isOnHookTm tm = tm == TmOnHook

data ControlMessages = CmSeize ProcessId | CmSeized | CmRejected | CmCleared | CmAnswered deriving (Data, Typeable, Eq)
instance Binary ControlMessages where
   get = genericGet
   put = genericPut
-- sent from the control processes 

isSeizeCm cm = case cm of 
                     CmSeize pid -> True
                     _ -> False


telephoneProcess :: TelephoneState -> ProcessM()
telephoneProcess state = receiveWait [
                            match (\msg -> 
                                     case msg of
                                        TmOffHook -> telephoneProcess IsOffHook
                                        TmOnHook -> telephoneProcess IsOnHook
                                        TmDigitDialed digit -> telephoneProcess state  -- TODO: send a digit message somewhere ...
                                  )
                         ]

initialProcess "NODE" = do
                           spawn (telephoneProcess IsOnHook)
                           return ()

-- state functions: each corresponds to a state in the POTS state machine

idle :: HardwareAddress -> ProcessM ()

idle adr = receiveWait [
-- Our phone is on-hook and no calls are in process to or from it

               match (\msg -> case msg of 
                                   CmSeize pidForA -> do
                                       -- A is trying to call us
                                       send pidForA CmSeized   -- let them know
                                       startRing adr         -- start our bell ringing
                                       ringingBSide adr pidForA  -- next state
                                   _ -> idle adr ),            -- stay in this state
               match (\msg -> case msg of
                                   TmOffHook -> do
                                       -- our handset has been lifted
                                       startTone adr DialTone
                                       gettingNumber adr []  -- next state
                                   _ -> idle adr )             -- stay in this state
         ]

-- Note the code duplication of "idle a".  This is because the first wildcard match will 
-- match against "other" messages that are of type ControlMessage, while the second will
-- match against those that are TelephoneMessages.  The code for idle' below is a
-- another way of saying, this without the duplication.

-- Will all of the "idle a" calls be compiled as tail calls?  This is essential!


idle' adr = receiveWait [
               matchIf isSeizeCm 
                       (\msg -> case msg of 
                                   CmSeize pid -> do
                                       send pid CmSeized
                                       startRing adr
                                       ringingBSide adr pid ),
               matchIf isOffHookTm
                       (\msg -> do 
                           startTone adr DialTone
                           gettingNumber adr [] ),
               matchOther (idle adr) -- Note that "other" may be a TelephoneMessage or a ControlMessage; we ignore both kinds
         ]

gettingNumber :: HardwareAddress -> String -> ProcessM ()

gettingNumber adr phoneNumber =
-- Our handset (the A-side) is off-hook and we are in the 
-- process of collecting the digits that will make up the
-- number that our handset wishes to call. If phoneNumber is null,
-- the first digit has yet to be dialed, and the A-side is hearing
-- the dial tone; otherwise, phoneNumber is a list of the digits 
-- that have already been dialed, and no tone is heard.

   receiveWait [
      match (\msg -> case msg of
                        TmDigitDialed d -> do
                              maybeStopTone adr phoneNumber
                              result <- analyse newNumber
                              case result of
                                 ArGetMoreDigits -> gettingNumber adr newNumber
                                 ArInvalid ->   do startTone adr FaultTone 
                                                   waitForOnHook adr (Just FaultTone)
                                 ArOK pidForB adrsForB ->
                                                do self <- getSelfPid
                                                   send pidForB (CmSeize self)
                                                   makeCallToB adr pidForB adrsForB
                              where newNumber = phoneNumber ++ [d]
                        TmOnHook -> do
                              maybeStopTone adr phoneNumber
                              idle adr
                        _ -> gettingNumber adr phoneNumber ),
      match (\msg -> case msg of 
                        CmSeize pidForA -> do
                              send pidForA CmRejected
                              gettingNumber adr phoneNumber
                        _ -> gettingNumber adr phoneNumber ) ]

makeCallToB :: HardwareAddress -> ProcessId -> HardwareAddress -> ProcessM ()

makeCallToB adr pidOfB adrOfB = 
-- we have just asked to establish a connectionwith the B side,
-- and are waiting for a response

   receiveWait [
      match (\msg -> case msg of
                        CmSeized -> do startTone adr RingTone
                                       ringingASide adr pidOfB adrOfB
                        CmRejected -> do  startTone adr BusyTone
                                          waitForOnHook adr (Just BusyTone)
                        CmSeize pid -> do send pid CmRejected
                                          makeCallToB adr pidOfB adrOfB ) ]
                                                      
                                          
ringingASide :: HardwareAddress -> ProcessId -> HardwareAddress -> ProcessM ()
                                         
ringingASide adr pidOfB adrOfB =
-- we have initiated a call; we are hearing the ring tone, and the other phone
-- (B-side) is ringing
   receiveWait [
      match (\msg -> case msg of
                        TmOnHook -> do send pidOfB CmCleared
                                       stopTone adr
                                       idle adr
                        _ -> ringingASide adr pidOfB adrOfB ),
      match (\msg -> case msg of
                        CmAnswered  -> do stopTone adr
                                          connect adr adrOfB
                                          speech adr pidOfB adrOfB
                        CmSeize pid -> do send pid CmRejected
                                          ringingASide adr pidOfB adrOfB
                        _ -> ringingASide adr pidOfB adrOfB ) ]
                        
                     

ringingBSide :: HardwareAddress -> ProcessId -> ProcessM ()

ringingBSide adr pidOfA = 
-- We have accepted a sieze request from the A-side, and our handset is ringing
   receiveWait [
      match (\msg -> case msg of 
                        CmCleared -> do stopRing adr
                                        idle adr
                        CmSeize pidOfA -> do send pidOfA CmRejected
                                             ringingBSide adr pidOfA
                        _ -> ringingBSide adr pidOfA ),
      match (\msg -> case msg of
                        TmOffHook -> do   stopRing adr
                                          send pidOfA CmAnswered
                                          speech adr pidOfA notAnAddress
                        _ -> ringingBSide adr pidOfA ) ]
                        
speech :: HardwareAddress -> ProcessId -> HardwareAddress -> ProcessM ()

speech myAdr otherPid otherAdr =
-- Both sides of this call enter this state, and the parties can talk
-- When the A-side enters this state, otherAdr is the HW address of the B-side.
-- When the B-side enters this state, otherAdr is notAnAddress
   receiveWait [
      match (\msg -> case msg of 
                        TmOnHook -> do send otherPid CmCleared
                                       maybeDisconnect myAdr otherAdr
                                       idle myAdr
                        _ -> speech myAdr otherPid otherAdr ) ,
      match (\msg -> case msg of
                        CmCleared -> do maybeDisconnect myAdr otherAdr
                                        waitForOnHook myAdr Nothing
                        _ -> speech myAdr otherPid otherAdr ) ]


waitForOnHook :: HardwareAddress -> Maybe ToneType -> ProcessM ()

waitForOnHook adr toneOpt =
-- we are waiting for adr to hang up
   receiveWait [
      match (\msg -> case msg of
                        TmOnHook -> do case toneOpt of 
                                          Nothing -> return ()
                                          _ -> stopTone adr
                                       idle adr
                        _ -> waitForOnHook adr toneOpt ) ]
      -- a control message should also be ignored.  How should we do that?
                        


-- auxiliary functions

maybeStopTone adr [] = stopTone adr
maybeStopTone _   _  = return ()


maybeDisconnect adr1 adr2 =   if adr2 == notAnAddress
                              then return ()
                              else disconnect adr1 adr2





