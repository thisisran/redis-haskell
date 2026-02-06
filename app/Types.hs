{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Types
  ( Parser
  , Command (..)
  , SetExpiry (..)
  , PushCommand (..)
  , EntryId (..)
  , RangeEntryId (..)
  , App
  , ExpireDuration (..)
  , ExpireReference (..)
  , MemoryStoreValue (..)
  , MemoryStoreEntry (..)
  , MemoryStore (..)
  , ClientState (..)
  , RedisStreamValues
  , RedisStream
  , RedisStreams
  , Stream (..)
  , Streams (..)
  , runApp
  ) where

import Data.Void (Void)
import Data.Word (Word64)
import Network.Simple.TCP (Socket)
import Text.Megaparsec (Parsec)


import Control.Monad.Reader
import Control.Monad.State.Strict

import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import qualified Data.IntSet as IS

import Control.Concurrent.STM (atomically, TVar, readTVar, writeTVar, newTVarIO, readTVarIO, modifyTVar')

type Parser = Parsec Void BS.ByteString

data SetExpiry = EX Int | PX Int
  deriving (Show, Eq)

data PushCommand = RightPushCmd | LeftPushCmd
  deriving (Show, Eq)

data Command
  = Ping
  | Echo BS.ByteString
  | Set BS.ByteString BS.ByteString (Maybe SetExpiry)
  | Get BS.ByteString
  | RPush BS.ByteString [BS.ByteString]
  | LPush BS.ByteString [BS.ByteString]
  | LRange BS.ByteString Int Int
  | LLen BS.ByteString
  | LPop BS.ByteString Int
  | BLPop BS.ByteString Double
  | Type BS.ByteString
  | XAdd BS.ByteString EntryId [(BS.ByteString, BS.ByteString)]
  | XRange BS.ByteString RangeEntryId RangeEntryId
  | XRead [(BS.ByteString, RangeEntryId)] (Maybe Double)
  | Incr BS.ByteString
  | Multi
  | Exec
  deriving (Show, Eq)

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
  , multiList :: [App BS.ByteString]
  , clientID :: !Int
  , socket :: Socket
  }

newtype App a = App { unApp :: ReaderT MemoryStore (StateT ClientState IO) a}
  deriving (Functor, Applicative, Monad, MonadIO, MonadState ClientState, MonadReader MemoryStore)

-- newtype App a = App { unApp :: StateT ClientState (ReaderT MemoryStore IO) a}
--   deriving (Functor, Applicative, Monad, MonadIO, MonadState ClientState, MonadReader MemoryStore)

runApp  :: MemoryStore -> ClientState -> App a -> IO (a, ClientState)
runApp store st = (`runStateT` st) . (`runReaderT` store) . unApp
