{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedRecordDot #-}

module MemoryStore
  ( MemoryStoreEntry (..)
  , MemoryStore (..)
  , ClientState (..)
  , MemoryStoreValue (..)
  , App
  , runApp
  , getSocket
  , ExpireDuration (..)
  , ExpireReference (..)
  , getClientID
  , getMulti
  , updateMulti 
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
  , EntryId (..)
  , Stream (..)
  , Streams (..)
  , RangeEntryId (..)
  , RedisStreams
  , RedisStream
  , RedisStreamValue
  , RedisStreamValues
  ) where

import Control.Concurrent.STM (atomically, TVar, readTVar, writeTVar, newTVarIO, readTVarIO, modifyTVar')

import Data.Maybe (fromMaybe)

import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import qualified Data.IntSet as IS

import Control.Monad (unless)

import Control.Monad.State.Strict
import Control.Monad.Reader

import Network.Simple.TCP (Socket)

import qualified Data.ByteString as BS

import Data.Word (Word64)

data EntryId = EntryId !Word64 !Word64
             | EntryGenSeq !Word64
             | EntryGenNew
             deriving (Eq, Ord, Show)

data RangeEntryId = RangeMinusPlus
                  | RangeDollar
                  | RangeMili !Word64
                  | RangeEntryId !Word64 !Word64
                  deriving (Eq, Show)

newtype Stream a    = Stream (M.Map EntryId a)
                      deriving (Eq, Show)
newtype Streams n a = Streams (HM.HashMap n (Stream a))
                      deriving (Eq, Show)

type RedisStreamValue = (BS.ByteString, BS.ByteString)
type RedisStreamValues = [RedisStreamValue]
type RedisStream = Stream RedisStreamValues
type RedisStreams = Streams BS.ByteString RedisStreamValues

newtype ExpireDuration = ExpireDuration Integer deriving (Eq, Show)  -- in miliseconds
newtype ExpireReference = ExpireReference Integer deriving (Eq, Show)

data MemoryStoreValue = MSIntegerVal Integer
                      | MSStringVal BS.ByteString
                      | MSListVal [BS.ByteString]
                      | MSStreams RedisStreams
                      deriving (Eq, Show)

data MemoryStoreEntry = MemoryStoreEntry
  { val :: MemoryStoreValue,
    expiresAt :: Maybe (ExpireDuration, ExpireReference)
  } deriving (Eq, Show)

data MemoryStore = MemoryStore
  { msData :: TVar (M.Map BS.ByteString MemoryStoreEntry)
  , msBLPopWaiters :: TVar (M.Map BS.ByteString IS.IntSet)
  }

data ClientState = ClientState
  { multi :: !Bool
  , clientID :: !Int
  , socket :: Socket }

newtype App a = App { unApp :: ReaderT MemoryStore (StateT ClientState IO) a}
  deriving (Functor, Applicative, Monad, MonadIO, MonadState ClientState, MonadReader MemoryStore)

-- newtype App a = App { unApp :: StateT ClientState (ReaderT MemoryStore IO) a}
--   deriving (Functor, Applicative, Monad, MonadIO, MonadState ClientState, MonadReader MemoryStore)

runApp  :: MemoryStore -> ClientState -> App a -> IO (a, ClientState)
runApp store st = (`runStateT` st) . (`runReaderT` store) . unApp

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

getMulti :: App Bool
getMulti = gets (.multi )

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
