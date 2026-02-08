{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}

module MemoryStore
  ( getSocket
  , getReplication
  , getClientReplication
  , getPort
  , getClientID
  , getMulti
  , getMultiList
  , updateMulti
  , addMultiCommand
  , resetMultiCommands
  , getData
  , getDataEntry
  , setDataEntry
  , addWaiterOnce
  , getWaiterEntry
  , delDataEntry
  , delWaiterEntry
  , newMemoryStore
  , getStreams
  , getStream
  , setStreams
  , getRole
  ) where

import Control.Concurrent.STM (atomically, TVar, readTVar, writeTVar, newTVarIO, readTVarIO, modifyTVar')

import Data.Maybe (fromMaybe)

import Types

import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import qualified Data.IntSet as IS

import Control.Monad.State.Strict
import Control.Monad.Reader

import Network.Simple.TCP (Socket)

import qualified Data.ByteString as BS

getData :: ClientApp (TVar (M.Map BS.ByteString MemoryStoreEntry))
getData = asks $ (.msData) . senvStore . cenvShared

getWaiters :: ClientApp (TVar (M.Map BS.ByteString IS.IntSet))
getWaiters = asks $ (.msBLPopWaiters) . senvStore . cenvShared

setDataEntry :: BS.ByteString -> MemoryStoreEntry -> ClientApp ()
setDataEntry key value = do
  tv <- getData
  liftIO . atomically $ modifyTVar' tv (M.insert key value)

getDataEntry :: BS.ByteString -> ClientApp (Maybe MemoryStoreEntry)
getDataEntry key = do
  tv <- getData
  liftIO $ M.lookup key <$> readTVarIO tv

delDataEntry :: BS.ByteString -> ClientApp ()
delDataEntry key = do
  tv <- getData
  liftIO . atomically $ do
    m0 <- readTVar tv
    let m1 = M.delete key m0
    writeTVar tv m1

delWaiterEntry :: BS.ByteString -> ClientApp ()
delWaiterEntry key = do
  tv <- getWaiters
  liftIO . atomically $ do
    m0 <- readTVar tv
    let m1 = M.delete key m0
    writeTVar tv m1

addWaiterOnce :: BS.ByteString -> Int -> ClientApp ()
addWaiterOnce k w = do
  tv <- getWaiters
  liftIO . atomically $
    modifyTVar' tv (M.insertWith IS.union k (IS.singleton w))

getWaiterEntry :: BS.ByteString -> ClientApp (Maybe IS.IntSet)
getWaiterEntry key = do
  tv <- getWaiters
  liftIO $ M.lookup key <$> readTVarIO tv

getRole :: ClientApp ReplicationInfo
getRole = asks $ (.cfgReplication) . ccfgShared . cenvConfig

getClientID :: ClientApp Int
getClientID = asks $ (.ccfgID) . cenvConfig

getSocket :: ClientApp Socket
getSocket = asks $ (.ccfgSocket) . cenvConfig

getReplication :: App ReplicationInfo
getReplication = asks $ (.cfgReplication) . senvConfig

getClientReplication :: ClientApp ReplicationInfo
getClientReplication = asks $ (.cfgReplication) . ccfgShared . cenvConfig

getPort :: App String
getPort = asks $ (.cfgPort) . senvConfig

newMemoryStore :: IO MemoryStore
newMemoryStore = MemoryStore <$> newTVarIO M.empty <*> newTVarIO M.empty

getStreams :: ClientApp RedisStreams
getStreams = do
  streams <- getDataEntry "streams"
  pure $ case streams of
    Just (MemoryStoreEntry (MSStreams s) Nothing) -> s
    _ -> Streams HM.empty

getStream ::
  BS.ByteString ->
  (M.Map EntryId RedisStreamValues -> Maybe (EntryId, RedisStreamValues)) ->
  ClientApp (Maybe (EntryId, RedisStreamValues), RedisStream, RedisStreams)
getStream streamID filter = do
  s@(Streams streams) <- getStreams
  let os@(Stream oldStream) = fromMaybe (Stream M.empty) (HM.lookup streamID streams)
  pure (filter oldStream, os, s)

setStreams :: MemoryStoreEntry -> ClientApp ()
setStreams = setDataEntry "streams"

----------------------------------------------------------------------------------
-- ClientState

updateMulti :: Bool -> ClientApp ()
updateMulti state = modify' (\cs -> cs { multi = state })

addMultiCommand :: ClientApp BS.ByteString -> ClientApp ()
addMultiCommand cmd = do
  ml <- gets (.multiList)
  modify' (\cs -> cs { multiList = ml ++ [cmd] })

resetMultiCommands :: ClientApp ()
resetMultiCommands = modify' (\cs -> cs { multiList = [] })

getMulti :: ClientApp Bool
getMulti = gets (.multi)

getMultiList :: ClientApp [ClientApp BS.ByteString]
getMultiList = gets (.multiList)
