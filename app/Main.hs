{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where


import Control.Monad.Trans.Except (runExceptT)

import CliParser
import qualified Control.Concurrent.Async as SA
import Control.Concurrent.STM
import qualified Control.Exception as CE
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as BS8
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import qualified Data.Set as S
import Store
import Network.Simple.TCP (HostPreference (HostAny), closeSock, recv, send, serve)
import RDBParser
import RedisParser
import System.FilePath ((</>))
import Control.Exception (catchJust)
import System.IO (BufferMode (NoBuffering), Handle, IOMode (ReadMode), hPutStrLn, hSetBuffering, stderr, stdout, withBinaryFile)
import System.IO.Error (ioeGetErrorType, isDoesNotExistErrorType)
import Types
import qualified Utilities as U

import Command
import Replica

handleClientConnection :: ClientApp ()
handleClientConnection = go mempty
  where
    go acc = do
       sock <- getSocket
       case parseOneCommand acc of
         RParserNeedMore -> do
           mb <- liftIO $ recv sock 4096
           case mb of
             Just buf -> go (acc <> buf)
             Nothing -> pure ()  -- TODO: return an error here summarizing what happened, and handle it at main
         RParsed cmd rest -> do
           runClientCommand cmd
           go rest -- rest may already contain next command
         RParserErr e -> liftIO $ send sock e

initSharedEnv :: Store -> SharedConfig -> IO SharedEnv
initSharedEnv store sharedCfg = do
  newZSets <- newTVarIO HM.empty
  newReplicas <- newTVarIO []
  sentOffset <- newTVarIO 0
  complReplicas <- newTVarIO 0
  newChannels <- newTVarIO HM.empty
  newIsAuth <- newTVarIO False
  newAuthUsers <- newTVarIO HM.empty
  pure $ SharedEnv { senvStore = store
                   , senvSets = newZSets
                   , senvConfig = sharedCfg
                   , senvReplicas = newReplicas
                   , senvReplicaSentOffset = sentOffset
                   , senvCompleteReplicaCount = complReplicas
                   , senvChannels = newChannels
                   , senvIsAuth = newIsAuth
                   , senvAuthUsers = newAuthUsers }

readDBFile :: Store -> Handle -> IO (Either AppError ())
readDBFile store h = do
  magicWord <- BS.hGet h 5
  redisVersion <- BS.hGet h 4
  runExceptT $ consumeMetadata h
  runExceptT $ consumeDB h (sData store)

loadRdbFile :: FilePath -> Store -> IO ()
loadRdbFile path store = do
  rdbResultOrMissing <- liftIO $ catchJust (\e -> if isDoesNotExistErrorType (ioeGetErrorType e) then Just () else Nothing)
                                           (Right <$> withBinaryFile path ReadMode (readDBFile store))
                                           (\() -> pure (Left ()))   -- file missing

  case rdbResultOrMissing of
    Left () -> liftIO $ putStrLn "No RDB file found, starting with an empty DB"
    Right (Right _) -> liftIO $ putStrLn "RDB File loaded successfully"
    Right (Left (ErrRDBParser err)) -> liftIO $ hPutStrLn stderr $ "RDB File parsing failed, starting an empty DB. Error: " <> show err

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering

  cfgCli <- parseCli

  let port = fromMaybe "6379" (cliPort cfgCli)
  let dir = fromMaybe "/tmp/redis-files" (cliDir cfgCli)
  let rdbFileName = fromMaybe "dump.rdb" (cliFileName cfgCli)

  store <- newMemoryStore

  -- Here, in case of failure, the store will remain empty
  loadRdbFile (dir </> rdbFileName) store

  repID <- U.randomAlphaNum40BS
  let sharedCfg = case cliReplication cfgCli of
        WantMaster -> SharedConfig port dir rdbFileName (Master (BS8.unpack repID) 0)
        WantSlave wantHost wantPort -> SharedConfig port dir rdbFileName (Slave wantHost wantPort)

  sharedEnv <- initSharedEnv store sharedCfg

  resHandle <- case sharedCfg of
    SharedConfig _ _ _ (Slave _ _) -> Just <$> SA.async (do
      newReplicaOffset <- newTVarIO 0
      let replicaEnv = ReplicaEnv sharedEnv newReplicaOffset
      res <- runReplicaApp replicaEnv runReplica
      case res of
        Left err -> liftIO $ hPutStrLn stderr $ "Error running Replica: " <> show err
        Right _ -> pure ())
    _ -> pure Nothing

  putStrLn $ "Redis server listening on port " ++ port
  serve HostAny port $ \(socket, address) -> do
    putStrLn $ "successfully connected client: " ++ show address

    nextID <- newTVarIO (0 :: Int)
    clientID <- atomically $ do
      i <- readTVar nextID
      writeTVar nextID (i + 1)
      pure i

    let newUserData = UserData { name= "default"
                               , flags =["nopass"]
                               , passwords = [] }

    let clientCfg = ClientConfig { ccfgID = clientID
                                 , ccfgSocket = socket
                                 , ccfgShared = sharedCfg }

    let env = ClientEnv { cenvShared = sharedEnv, cenvConfig = clientCfg }

    let cs = ClientState { multi = False
                         , multiList = []
                         , subscribeMode = False
                         , subscribeChannels = S.empty
                         , userData = newUserData
                         , isAuth = False }

    runClientApp env cs handleClientConnection
    closeSock socket
