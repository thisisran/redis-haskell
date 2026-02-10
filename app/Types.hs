{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Types
  ( RParserResult (..)
  , Command (..)
  , StringParserResult (..)
  , TCPClientAckResult (..)
  , TCPReceivedResult (..)
  , ConfigArgs (..)
  , SetExpiry (..)
  , PushCommand (..)
  , EntryId (..)
  , RangeEntryId (..)
  , InfoRequest (..)
  , ReplConfOptions (..)
  , PSyncRequest (..)
  , ReplicaApp
  , ClientApp
  , ExpireDuration (..)
  , ExpireReference (..)
  , MemoryStoreValue (..)
  , MemoryStoreEntry (..)
  , MemoryStore (..)
  , ClientState (..)
  , Response (..)
  , emptyResponse
  , RedisStreamValues
  , RedisStream
  , RedisStreams
  , Stream (..)
  , Streams (..)
  , runReplicaApp
  , runClientApp
  , SharedConfig (..)
  , ClientConfig (..)
  , CLIOptions (..)
  , ReplicationInfo (..)
  , ReplicationCmdOption (..)
  , SharedEnv (..)
  , ClientEnv (..)
  , ReplicaEnv (..)
  , LengthEncoding (..)
  ) where

import Data.Void (Void)
import Data.Word (Word64)
import Network.Simple.TCP (Socket)

import UnliftIO (MonadUnliftIO)

import Control.Monad.Reader
import Control.Monad.State.Strict

import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import qualified Data.IntSet as IS

import Control.Concurrent.STM (atomically, TVar, readTVar, writeTVar, newTVarIO, readTVarIO, modifyTVar')

data RParserResult = RParsed !Command !BS.ByteString
                   | RParserNeedMore
                   | RParserErr !BS.ByteString
                   deriving stock (Show)

data LengthEncoding = SimpleString !Int
                    | ComplexString !Int
                    deriving (Eq, Show)

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

data ReplConfOptions = ListeningPort BS.ByteString
                     | Capa BS.ByteString
                     | GetAck
                     | AckWith Int
                     deriving (Eq, Show)

data PSyncRequest = PSyncUnknown
                  | PSyncFull BS.ByteString Word64
                  deriving (Eq, Show)

-- data ClientAckResult = ClientAckFull !Int !BS.ByteString
--                      | ClientAckPartial
--                      | ClientAckError !BS.ByteString
--                      deriving stock (Eq, Show)

data TCPClientAckResult = TCPAckResultFull !Int
                        | TCPAckResultError !BS.ByteString
                        deriving stock (Eq, Show)

data StringParserResult = SParserFullString !BS.ByteString !BS.ByteString
                        | SParserPartialString
                        | SParserError !BS.ByteString
                        deriving stock (Eq, Show)

data TCPReceivedResult = TCPResultFull !BS.ByteString !BS.ByteString
                       | TCPResultError !BS.ByteString
                       deriving stock (Eq, Show)

data ConfigArgs = ConfigGetDir
                | ConfigGetFileName
                deriving stock (Eq, Show)

data Command
  = Ping
  | Echo !BS.ByteString
  | Set !BS.ByteString !BS.ByteString (Maybe SetExpiry)
  | Get !BS.ByteString
  | RPush !BS.ByteString [BS.ByteString]
  | LPush !BS.ByteString [BS.ByteString]
  | LRange !BS.ByteString !Int !Int
  | LLen !BS.ByteString
  | LPop !BS.ByteString !Int
  | BLPop !BS.ByteString !Double
  | Type !BS.ByteString
  | XAdd !BS.ByteString !EntryId [(BS.ByteString, BS.ByteString)]
  | XRange !BS.ByteString !RangeEntryId !RangeEntryId
  | XRead [(BS.ByteString, RangeEntryId)] (Maybe Double)
  | Incr !BS.ByteString
  | Multi
  | Exec
  | Discard
  | Info !InfoRequest
  | ReplConf !ReplConfOptions
  | Psync !PSyncRequest
  | Wait !Int !Double
  | Config !ConfigArgs
  | Keys !BS.ByteString
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
  , cliDir         :: !(Maybe String)
  , cliFileName    :: !(Maybe String)
  , cliReplication :: !ReplicationCmdOption
  } deriving stock (Eq, Show)

data ReplicationInfo = Master { repID :: !String, repOffset :: !Int }
                     | Slave  { roHost :: !String, roPort :: !String }
                     deriving stock (Eq, Show)

data SharedConfig = SharedConfig
  { cfgPort        :: !String
  , cfgDir         :: !String
  , cfgRDBFileName :: !String
  , cfgReplication :: !ReplicationInfo
  } deriving stock (Eq, Show)

data SharedEnv = SharedEnv
  { senvStore             :: !MemoryStore
  , senvConfig            :: !SharedConfig
  , senvReplicas          :: TVar [Socket]
  , senvReplicaSentOffset :: TVar Int -- byte count of commands sent to replicas
  , completeReplicaCount  :: TVar Int
  }

data ClientConfig = ClientConfig
  { ccfgID     :: !Int
  , ccfgSocket :: !Socket
  , ccfgShared :: !SharedConfig
  }

data ReplicaEnv = ReplicaEnv
  { renvShared        :: !SharedEnv
  , renvReplicaOffset :: TVar Int
  }

data ClientEnv = ClientEnv
  { cenvShared   :: !SharedEnv
  , cenvConfig   :: !ClientConfig
  }

data ClientState = ClientState
  { multi :: !Bool
  , multiList :: [ClientApp Response]
  }

data Response = Response
  { rspBytes :: BS.ByteString
  , rspAfter :: ClientApp ()
  }

emptyResponse :: ClientApp ()
emptyResponse = pure ()

newtype ReplicaApp a = App { unReplicaApp :: ReaderT ReplicaEnv IO a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader ReplicaEnv, MonadUnliftIO)

newtype ClientApp a = ClientApp { unClientApp :: StateT ClientState (ReaderT ClientEnv IO) a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader ClientEnv, MonadState ClientState)

-- newtype App a = App { unApp :: StateT ClientState (ReaderT MemoryStore IO) a}
--   deriving (Functor, Applicative, Monad, MonadIO, MonadState ClientState, MonadReader MemoryStore)

runReplicaApp  :: ReplicaEnv -> ReplicaApp a -> IO a
runReplicaApp env = (`runReaderT` env) . unReplicaApp

runClientApp :: ClientEnv -> ClientState -> ClientApp a -> IO (a, ClientState)
runClientApp env st = (`runReaderT` env) . (`runStateT` st) . unClientApp
