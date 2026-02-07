{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Types
  ( RedisParser
  , Command (..)
  , SetExpiry (..)
  , PushCommand (..)
  , EntryId (..)
  , RangeEntryId (..)
  , InfoRequest (..)
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
  , Config (..)
  , CLIOptions (..)
  , ReplicationInfo (..)
  , ReplicationCmdOption (..)
  , Env (..)
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

type RedisParser = Parsec Void BS.ByteString

data SetExpiry = EX Int | PX Int
  deriving (Show, Eq)

data PushCommand = RightPushCmd | LeftPushCmd
  deriving (Show, Eq)

data InfoRequest = FullInfo
                 | Server
                 | Replication
                 | Clients
                 | Memory
                 deriving (Eq, Show)

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
  | Discard
  | Info InfoRequest
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

data ReplicationCmdOption = WantMaster
                          | WantSlave { rcoHost :: !String, rcoPort :: !String }
                          deriving stock (Eq, Show)

data CLIOptions = CLIOptions
  { cliPort        :: !(Maybe String)
  , cliReplication :: !ReplicationCmdOption
  } deriving stock (Eq, Show)

data ReplicationInfo = Master { repID :: !String, repOffset :: !Int }
                     | Slave  { roHost :: !String, roPort :: !String }
                     deriving stock (Eq, Show)

data Config = Config
  { cfgPort        :: !String
  , cfgID          :: !Int
  , cfgSocket      :: !Socket
  , cfgReplication :: !ReplicationInfo
  } deriving stock (Eq, Show)

data Env = Env
  { envStore :: !MemoryStore
  , envConfig :: !Config
  }

data ClientState = ClientState
  { multi :: !Bool
  , multiList :: [App BS.ByteString]
  }

newtype App a = App { unApp :: ReaderT Env (StateT ClientState IO) a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadState ClientState, MonadReader Env)

-- newtype App a = App { unApp :: StateT ClientState (ReaderT MemoryStore IO) a}
--   deriving (Functor, Applicative, Monad, MonadIO, MonadState ClientState, MonadReader MemoryStore)

runApp  :: Env -> ClientState -> App a -> IO (a, ClientState)
runApp store st = (`runStateT` st) . (`runReaderT` store) . unApp
