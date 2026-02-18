{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Replica
  ( runReplica )

  where

import Control.Monad.Trans.Except (ExceptT (..), runExceptT)
import Control.Monad.Reader (asks, ask)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Except (throwError, MonadError)
import Control.Monad.IO.Class (MonadIO, liftIO)

import UnliftIO (withRunInIO)

import Control.Monad (unless, void)

import Control.Concurrent.STM (atomically)

import Network.Simple.TCP (Socket, recv, send, connect)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import Types
import Store
import Command
import RedisParser
import Encode

newtype ReplicaError a = ReplicaError { runReplicaError :: ExceptT AppError ReplicaApp a }
                         deriving newtype (Functor, Applicative, Monad, MonadError AppError, MonadIO, MonadStore)

recvByParser :: (BS.ByteString -> StringParserResult) -> Socket -> BS.ByteString -> ReplicaError (BS.ByteString, BS.ByteString)
recvByParser parser sock = go
  where
    go :: BS.ByteString -> ReplicaError (BS.ByteString, BS.ByteString)
    go acc = do
      case parser acc of
        SParserFullString str rest -> pure (str, rest)
        SParserPartialString ->
          recv sock 4096 >>= \case
            Nothing -> throwError $ ErrNetwork "Received only partial response from the (Master) server"
            Just chunk -> go (acc <> chunk)
        SParserError msg -> throwError $ ErrParser $ "Replica: bad input ito parser: " <> msg

recvSimpleResponse :: Socket -> BS.ByteString -> ReplicaError (BS.ByteString, BS.ByteString)
recvSimpleResponse = recvByParser parseSimpleString

recvRDBFile :: Socket -> BS.ByteString -> ReplicaError (BS.ByteString, BS.ByteString)
recvRDBFile = recvByParser parseRDBFile

getAckCommand :: ReplicaError BS.ByteString
getAckCommand = do
  offset <- ReplicaError $ lift getReplicaOffset
  pure (encodeArray True ["REPLCONF", "ACK", (BS8.pack . show) offset])

awaitServerUpdates :: Socket -> BS.ByteString -> ReplicaError (BS.ByteString, BS.ByteString)
awaitServerUpdates sock = go 0
  where
    go :: Int -> BS.ByteString -> ReplicaError (BS.ByteString, BS.ByteString)
    go offset acc = do
     ReplicaError $ lift $ setReplicaOffset offset
     case parseOneCommand acc of
       RParsed cmd rest -> do
         let processedCount = BS.length acc - BS.length rest
         case cmd of
           Ping -> void $ pure ()
           Set key val args -> void $ ReplicaError $ lift $ setCommand key val args
           RPush key values -> void $ ReplicaError $ lift $ pushCommand key values RightPushCmd
           LPush key values -> void $ ReplicaError $ lift $ pushCommand key values LeftPushCmd
           LPop key count -> do
             tvStore  <- ReplicaError $ lift $ asks $ (.sData) . senvStore . renvShared
             void $ liftIO . atomically $ applyLPopHelper tvStore key count
           XAdd streamID entryID v ->
             void $ ReplicaError $ lift $ xaddCommand streamID entryID v
           Incr key -> void $ ReplicaError $ lift $ incrCommand key
           ReplConf GetAck -> do
             resp <- getAckCommand
             void $ liftIO (send sock resp)
           _ -> throwError $ ErrInternal "Error (Replica): unknown command from the server"
         go (offset + processedCount) rest
       RParserNeedMore -> do
         mbMore <- liftIO $ recv sock 4096
         case mbMore of
           Nothing -> throwError $ ErrNetwork "Error (Replica): Incomplete command received and server seemed to have closed the connection"
           Just more -> go offset (acc <> more)
       RParserErr _ -> throwError $ ErrNetwork "Error (Replica): Command received from server coud not be parsed properly"

runHandshake :: Socket -> ReplicaError BS.ByteString
runHandshake sock = do
  clientPort <- getPort
  liftIO $ send sock (encodeArray True ["PING"])

  (pongResp, pending0) <- recvSimpleResponse sock mempty

  unless (pongResp == "PONG") $
    throwError $ ErrNetwork "Replica: did not receive PONG back from the server"

  -- same rule for all the recv* functions:
  liftIO $ send sock (encodeArray True ["REPLCONF", "listening-port", BS8.pack clientPort])
  (replResp1, pending1) <- recvSimpleResponse sock pending0
  unless (replResp1 == "OK") $
    throwError $ ErrNetwork "Replica: expected OK after REPLCONF listening-port"

  liftIO $ send sock (encodeArray True ["REPLCONF", "capa", "psync2"])
  (replResp2, pending2) <- recvSimpleResponse sock pending1
  unless (replResp2 == "OK") $
    throwError $ ErrNetwork "Replica: expected OK after REPLCONF capa"

  liftIO $ send sock (encodeArray True ["PSYNC", "?", "-1"])
  (_replPsync, pending3) <- recvSimpleResponse sock pending2

  (_rdbFile, pending4) <- recvRDBFile sock pending3
  (resp, _pending5) <- awaitServerUpdates sock pending4
  pure resp

runReplica :: ReplicaApp (Either AppError BS.ByteString)
runReplica = do
  env <- ask
  getReplication >>= \case
    Master _ _ -> pure $ Left $ ErrInternal "Error: trying to run a Master node as Replica"
    -- withRunInIO :: ((ReplicaApp () -> IO ()) -> IO) -> ReplicaApp()
    Slave host port -> withRunInIO $ \runInIO -> do
      putStrLn ("Trying to connect to " <> host <> " " <> port)
      liftIO $ connect host port $ \(_sock, _addr) ->
        -- runInIO :: ReplicaApp () -> IO ()
        ((env `runReplicaApp`) . runExceptT . runReplicaError) $ runHandshake _sock
