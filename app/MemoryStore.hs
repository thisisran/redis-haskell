module MemoryStore
  ( MemoryStoreEntry (..)
  , MemoryStore
  , MemoryStoreValue (..)
  , newMemoryStore
  , getMemoryStoreVal
  , setMemoryStoreKey
  , delMemoryStoreKey
  ) where

import Control.Concurrent.STM (atomically, TVar, readTVar, writeTVar, newTVarIO, readTVarIO, modifyTVar')
import qualified Data.Map.Strict as M

import Types (ExpireDuration (..), ExpireReference (..))

import qualified Data.ByteString as BS

data MemoryStoreValue = MSStringVal BS.ByteString
                      | MSListVal [BS.ByteString]
                      deriving (Eq, Show)

data MemoryStoreEntry = MemoryStoreEntry
  { val :: MemoryStoreValue,
    expiresAt :: Maybe (ExpireDuration, ExpireReference)
  } deriving (Eq, Show)

newtype MemoryStore = MemoryStore (TVar (M.Map BS.ByteString MemoryStoreEntry))

newMemoryStore :: IO MemoryStore
newMemoryStore = MemoryStore <$> newTVarIO M.empty

getMemoryStoreVal :: MemoryStore -> BS.ByteString -> IO (Maybe MemoryStoreEntry)
getMemoryStoreVal (MemoryStore tv) k = M.lookup k <$> readTVarIO tv

setMemoryStoreKey :: MemoryStore -> BS.ByteString -> MemoryStoreEntry -> IO ()
setMemoryStoreKey (MemoryStore tv) k v = atomically $ modifyTVar' tv (M.insert k v)

delMemoryStoreKey :: MemoryStore -> BS.ByteString -> IO Bool
delMemoryStoreKey (MemoryStore tv) k = atomically $ do
  m <- readTVar tv
  let existed = M.member k m
  writeTVar tv (M.delete k m)
  pure existed
