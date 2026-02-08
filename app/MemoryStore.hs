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
  , MonadStore
  , getReplicas
  , addReplica
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

class (Monad m, MonadIO m) => MonadStore m where
  getData :: m (TVar (M.Map BS.ByteString MemoryStoreEntry))
  setDataEntry :: BS.ByteString -> MemoryStoreEntry -> m ()
  setDataEntry key value = do
    tv <- getData
    liftIO . atomically $ modifyTVar' tv (M.insert key value)
  getDataEntry :: BS.ByteString -> m (Maybe MemoryStoreEntry)
  getDataEntry key = do
    tv <- getData
    liftIO $ M.lookup key <$> readTVarIO tv
  getStream :: BS.ByteString -> (M.Map EntryId RedisStreamValues -> Maybe (EntryId, RedisStreamValues)) -> m (Maybe (EntryId, RedisStreamValues), RedisStream, RedisStreams)
  getStream streamID filter = do
    s@(Streams streams) <- getStreams
    let os@(Stream oldStream) = fromMaybe (Stream M.empty) (HM.lookup streamID streams)
    pure (filter oldStream, os, s)
  getStreams :: m RedisStreams
  getStreams = do
    streams <- getDataEntry "streams"
    pure $ case streams of
      Just (MemoryStoreEntry (MSStreams s) Nothing) -> s
      _ -> Streams HM.empty
  setStreams :: MemoryStoreEntry -> m ()
  setStreams = setDataEntry "streams"
  getPort :: m String
  getReplication :: m ReplicationInfo
  getReplicas :: m (TVar [Socket])
  addReplica :: Socket -> m ()
  addReplica sock = do
    tv <- getReplicas
    liftIO . atomically $ modifyTVar' tv (\l -> l ++ [sock])

-------------------------------------------------------------------------------------


getWaiters :: ClientApp (TVar (M.Map BS.ByteString IS.IntSet))
getWaiters = asks $ (.msBLPopWaiters) . senvStore . cenvShared

instance MonadStore App where
  getData = asks $ (.msData) . senvStore
  getPort = asks $ (.cfgPort) . senvConfig
  getReplication = asks $ (.cfgReplication) . senvConfig
  getReplicas = asks cenvReplicas

instance MonadStore ClientApp where
  getData = asks $ (.msData) . senvStore . cenvShared
  getPort = asks $ (.cfgPort) . senvConfig . cenvShared
  getReplication = asks $ (.cfgReplication) . senvConfig . cenvShared
  getReplicas = asks (cenvReplicas . cenvShared)
    

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

getClientReplication :: ClientApp ReplicationInfo
getClientReplication = asks $ (.cfgReplication) . ccfgShared . cenvConfig

---------

newMemoryStore :: IO MemoryStore
newMemoryStore = MemoryStore <$> newTVarIO M.empty <*> newTVarIO M.empty

----------------------------------------------------------------------------------
-- ClientState

updateMulti :: Bool -> ClientApp ()
updateMulti state = modify' (\cs -> cs { multi = state })

addMultiCommand :: ClientApp Response -> ClientApp ()
addMultiCommand cmd = do
  ml <- gets (.multiList)
  modify' (\cs -> cs { multiList = ml ++ [cmd] })

resetMultiCommands :: ClientApp ()
resetMultiCommands = modify' (\cs -> cs { multiList = [] })

getMulti :: ClientApp Bool
getMulti = gets (.multi)

getMultiList :: ClientApp [ClientApp Response]
getMultiList = gets (.multiList)
