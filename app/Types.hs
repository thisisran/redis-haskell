{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Types
  ( RParserResult (..)
  , Command (..)
  , DistUnit (..)
  , AppError (..)
  , AclSubCmd (..)
  , UserFlags
  , UserPasswords
  , UserData (..)
  , StringParserResult (..)
  , RespError (..)
  , Effect (..)
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
  , ExDurationMs (..)
  , ExRef (..)
  , StoreValue (..)
  , StoreEntry (..)
  , Store (..)
  , StoreData
  , ClientState (..)
  , Response (..)
  , BSPair
  , Stream
  , Streams
  , runReplicaApp
  , runClientApp
  , SharedConfig (..)
  , ZSetScoreMap
  , ZSetMemberDict
  , ZSet (..)
  , ZSets
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
import qualified Data.HashSet as HS
import qualified Data.Set as S
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

data RespError = RErrXAddGtThan0
               | RErrXaddEqSmallTargetItem
               | RErrXRangeIDNonExisting
               | RErrIncrNotIntegerOrRange
               | RErrExecNoMulti
               | RErrDiscardNoMulti
               | RErrGeoAddLongRange
               | RErrGeoAddLatRange
               | RErrGeoDistMissingMember
               | RErrAuthInvalidUserName
               | RErrAuthServerAuthUserNotFound
               | RErrAuthRequired
               | RErrSubUnauthorizedCmd
               | RErrPopWrongValueType
               deriving stock (Eq, Show)

data TCPReceivedResult = TCPResultFull !BS.ByteString !BS.ByteString
                       | TCPResultError !BS.ByteString
                       deriving stock (Eq, Show)

data ConfigArgs = ConfigGetDir
                | ConfigGetFileName
                deriving stock (Eq, Show)

data DistUnit = DistMeter
              | DistKilometer
              | DistMile
              deriving stock (Eq, Show)

data AclSubCmd = AclWhoAmI
               | AclGetUser !BS.ByteString
               | AclSetUser !BS.ByteString !BS.ByteString
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
  | Subscribe !BS.ByteString
  | Publish !BS.ByteString !BS.ByteString
  | Unsubscribe !BS.ByteString
  | ZAdd !BS.ByteString !Double !BS.ByteString
  | ZRank !BS.ByteString !BS.ByteString
  | ZRange !BS.ByteString !Int !Int
  | ZCard !BS.ByteString
  | ZScore !BS.ByteString !BS.ByteString
  | ZRem !BS.ByteString !BS.ByteString
  | GeoAdd !BS.ByteString Double Double !BS.ByteString
  | GeoPos !BS.ByteString ![BS.ByteString]
  | GeoDist !BS.ByteString !BS.ByteString !BS.ByteString
  | GeoSearch !BS.ByteString Double Double Double DistUnit
  | Acl AclSubCmd
  | Auth !BS.ByteString !BS.ByteString
  | Cmd
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

newtype ExDurationMs = ExDurationMs Integer deriving (Eq, Show)  -- in miliseconds
newtype ExRef = ExRef Integer deriving (Eq, Show)

data StoreValue = StoreString BS.ByteString
                | StoreList [BS.ByteString]
                deriving (Eq, Show)

data StoreEntry = StoreEntry
  { val :: StoreValue
  , expiresAt :: Maybe (ExDurationMs, ExRef)
  } deriving (Eq, Show)

type StoreData = M.Map BS.ByteString StoreEntry

type BSPair = (BS.ByteString, BS.ByteString)
type Stream = M.Map EntryId [BSPair]
type Streams = HM.HashMap BS.ByteString Stream

data Store = Store
  { sData :: TVar StoreData
  , sStreams :: TVar Streams
  } deriving stock (Eq)

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

type ZSetMemberDict = (HM.HashMap BS.ByteString Double)
type ZSetScoreMap = (M.Map Double (S.Set BS.ByteString))
data ZSet = ZSet ZSetScoreMap ZSetMemberDict
type ZSets = TVar (HM.HashMap BS.ByteString ZSet)

type UserFlags = [BS.ByteString]
type UserPasswords = [BS.ByteString]
data UserData = UserData { name :: !BS.ByteString
                         , flags :: !UserFlags
                         , passwords :: !UserPasswords }
                         deriving stock (Eq, Show)

data SharedEnv = SharedEnv
  { senvStore                :: !Store
  , senvSets                 :: !ZSets
  , senvConfig               :: !SharedConfig
  , senvReplicas             :: TVar [Socket]
  , senvReplicaSentOffset    :: TVar Int -- byte count of commands sent to replicas
  , senvCompleteReplicaCount :: TVar Int
  , senvChannels             :: TVar (HM.HashMap BS.ByteString [Socket])
  , senvIsAuth               :: TVar Bool
  , senvAuthUsers            :: TVar (HM.HashMap BS.ByteString BS.ByteString)
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
  { multi             :: !Bool
  , multiList         :: [ClientApp Response]
  , subscribeMode     :: !Bool
  , subscribeChannels :: !(S.Set BS.ByteString)
  , userData          :: !UserData
  , isAuth            :: !Bool
  }

data AppError = ErrNetwork !BS.ByteString
              | ErrParser !BS.ByteString
              | ErrInternal !BS.ByteString
              deriving stock (Eq, Show)

data Effect = EffUpdateReplica ![BS.ByteString]
            | EffPublishChannel !BS.ByteString !BS.ByteString
            | EffSnapshot

data Response = RspNormal { resp :: !BS.ByteString, cmdName :: !BS.ByteString }
              | RspSubs !BS.ByteString
              | RspContinue { resp :: !BS.ByteString, effect :: Effect, cmdName :: !BS.ByteString }
              | RspContinueSubs { resp :: !BS.ByteString, effect :: Effect }
              | RspPing

newtype ReplicaApp a = App { unReplicaApp :: ReaderT ReplicaEnv IO a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader ReplicaEnv, MonadUnliftIO)

newtype ClientApp a = ClientApp { unClientApp :: StateT ClientState (ReaderT ClientEnv IO) a }
  deriving newtype (Functor, Applicative, Monad, MonadIO, MonadReader ClientEnv, MonadState ClientState)

runReplicaApp  :: ReplicaEnv -> ReplicaApp a -> IO a
runReplicaApp env = (`runReaderT` env) . unReplicaApp

runClientApp :: ClientEnv -> ClientState -> ClientApp a -> IO (a, ClientState)
runClientApp env st = (`runReaderT` env) . (`runStateT` st) . unClientApp
