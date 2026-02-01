module MemoryStore
  ( MemoryStoreEntry (..)
  , MemoryStore
  , MemoryStoreValue (..)
  , BLPopWaiter (..)
  , newMemoryStore
  , getMemoryDataVal
  , setMemoryDataKey
  , delMemoryDataKey
  , getMemoryWaitersVal
  , setMemoryWaitersKey
  , delMemoryWaitersKey
  , addMemoryWaiter
  ) where

import Control.Concurrent.STM (atomically, TVar, readTVar, writeTVar, newTVarIO, readTVarIO, modifyTVar')
import qualified Data.Map.Strict as M

import Control.Monad (unless)

import qualified Types as T

import qualified Data.ByteString as BS

data MemoryStoreValue = MSStringVal BS.ByteString
                      | MSListVal [BS.ByteString]
                      deriving (Eq, Show)

newtype BLPopWaiter = BLPopWaiter T.ClientID deriving (Eq, Show)
type BLPopWaiters = [BLPopWaiter]

data MemoryStoreEntry = MemoryStoreEntry
  { val :: MemoryStoreValue,
    expiresAt :: Maybe (T.ExpireDuration, T.ExpireReference)
  } deriving (Eq, Show)

data MemoryStore = MemoryStore
  { msData :: TVar (M.Map BS.ByteString MemoryStoreEntry)
  , msBLPopWaiters :: TVar (M.Map BS.ByteString BLPopWaiters)
  }

newMemoryStore :: IO MemoryStore
newMemoryStore = MemoryStore <$> newTVarIO M.empty <*> newTVarIO M.empty

getMemoryDataVal :: MemoryStore -> T.DataKey -> IO (Maybe MemoryStoreEntry)
getMemoryDataVal (MemoryStore d w) k = M.lookup k <$> readTVarIO d

setMemoryDataKey :: MemoryStore -> T.DataKey -> MemoryStoreEntry -> IO ()
setMemoryDataKey (MemoryStore d w) k v = atomically $ modifyTVar' d (M.insert k v)

delMemoryDataKey :: MemoryStore -> T.DataKey -> IO Bool
delMemoryDataKey (MemoryStore d w) k = atomically $ do
  m <- readTVar d
  let existed = M.member k m
  writeTVar d (M.delete k m)
  pure existed

getMemoryWaitersVal :: MemoryStore -> T.ListKey -> IO (Maybe BLPopWaiters)
getMemoryWaitersVal (MemoryStore d w) k = M.lookup k <$> readTVarIO w

setMemoryWaitersKey :: MemoryStore -> T.ListKey -> BLPopWaiters -> IO ()
setMemoryWaitersKey (MemoryStore d w) k v = atomically $ modifyTVar' w (M.insert k v)

addMemoryWaiter :: MemoryStore -> T.ListKey -> T.ClientID -> IO ()
addMemoryWaiter store listKey cid = do
  val <- getMemoryWaitersVal store listKey
  case val of
    Nothing -> insertNewClient []
    Just xs -> unless (foldr checkEq False xs) $ insertNewClient xs
               where checkEq _ True = True
                     checkEq (BLPopWaiter curr) acc = curr == cid
  where insertNewClient ys = setMemoryWaitersKey store listKey (ys ++ [BLPopWaiter cid])

delMemoryWaitersKey :: MemoryStore -> T.ListKey -> IO Bool
delMemoryWaitersKey (MemoryStore d w) k = atomically $ do
  m <- readTVar w
  let existed = M.member k m
  writeTVar w (M.delete k m)
  pure existed
