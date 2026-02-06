{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE OverloadedRecordDot #-}

module MemoryStore
  ( getSocket
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

getData :: App (TVar (M.Map BS.ByteString MemoryStoreEntry))
getData = asks (.msData)

getWaiters :: App (TVar (M.Map BS.ByteString IS.IntSet))
getWaiters = asks (.msBLPopWaiters )

setDataEntry :: BS.ByteString -> MemoryStoreEntry -> App ()
setDataEntry key value = do
  tv <- getData
  liftIO . atomically $ modifyTVar' tv (M.insert key value)

getDataEntry :: BS.ByteString -> App (Maybe MemoryStoreEntry)
getDataEntry key = do
  tv <- getData
  liftIO $ M.lookup key <$> readTVarIO tv

delDataEntry :: BS.ByteString -> App ()
delDataEntry key = do
  tv <- getData
  liftIO . atomically $ do
    m0 <- readTVar tv
    let m1 = M.delete key m0
    writeTVar tv m1

delWaiterEntry :: BS.ByteString -> App ()
delWaiterEntry key = do
  tv <- getWaiters
  liftIO . atomically $ do
    m0 <- readTVar tv
    let m1 = M.delete key m0
    writeTVar tv m1

addWaiterOnce :: BS.ByteString -> Int -> App ()
addWaiterOnce k w = do
  tv <- getWaiters
  liftIO . atomically $
    modifyTVar' tv (M.insertWith IS.union k (IS.singleton w))

getWaiterEntry :: BS.ByteString -> App (Maybe IS.IntSet)
getWaiterEntry key = do
  tv <- getWaiters
  liftIO $ M.lookup key <$> readTVarIO tv

updateMulti :: Bool -> App ()
updateMulti state = modify' (\cs -> cs { multi = state })

addMultiCommand :: App BS.ByteString -> App ()
addMultiCommand cmd = do
  ml <- gets (.multiList)
  modify' (\cs -> cs { multiList = ml ++ [cmd] })

resetMultiCommands :: App ()
resetMultiCommands = modify' (\cs -> cs { multiList = [] })

getMulti :: App Bool
getMulti = gets (.multi )

getMultiList :: App [App BS.ByteString]
getMultiList = gets (.multiList)

getClientID :: App Int
getClientID = gets (.clientID)

getSocket :: App Socket
getSocket = gets (.socket)

newMemoryStore :: IO MemoryStore
newMemoryStore = MemoryStore <$> newTVarIO M.empty <*> newTVarIO M.empty

getStreams :: App RedisStreams
getStreams = do
  streams <- getDataEntry "streams"
  pure $ case streams of
    Just (MemoryStoreEntry (MSStreams s) Nothing) -> s
    _ -> Streams HM.empty

getStream ::
  BS.ByteString ->
  (M.Map EntryId RedisStreamValues -> Maybe (EntryId, RedisStreamValues)) ->
  App (Maybe (EntryId, RedisStreamValues), RedisStream, RedisStreams)
getStream streamID filter = do
  s@(Streams streams) <- getStreams
  let os@(Stream oldStream) = fromMaybe (Stream M.empty) (HM.lookup streamID streams)
  pure (filter oldStream, os, s)

setStreams :: MemoryStoreEntry -> App ()
setStreams = setDataEntry "streams"
