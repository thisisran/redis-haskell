{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import System.FilePath ((</>))
import System.Environment (getArgs)
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout, withBinaryFile, IOMode(ReadMode), Handle, SeekMode(RelativeSeek), hSeek)

import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import Control.Monad.Trans.Except (ExceptT (..), runExceptT)
import Control.Monad.Except (throwError)

import qualified Control.Concurrent.Async as SA
-- import Control.Exception (IOException)
import qualified Control.Exception as CE
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Monad (when, unless, void)

import Control.Monad.State.Strict (gets, modify')
import Control.Monad.Reader (asks)

import qualified Crypto.Hash.SHA256 as SHA256
import qualified Data.ByteString.Base16 as B16

import UnliftIO (MonadUnliftIO, withRunInIO)
import qualified UnliftIO as UL

import RDBParser

import Data.List (foldl', delete)
import Data.Word (Word64)
import Data.Maybe (fromMaybe, isNothing, isJust)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve, connect)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.Set as S
import qualified Data.HashMap.Strict as HM
import qualified Data.IntSet as IS
import qualified Data.Map.Strict as M

import Encode
import MemoryStore
import qualified Utilities as U

import Types
import RedisParser
import CliParser

isMaster :: MonadStore m => m Bool
isMaster = do
  getReplication >>= \case
    (Master _ _) -> pure True
    _            -> pure False

updateReplicas :: MonadStore m => [BS.ByteString] -> m ()
updateReplicas command = do
  update <- isMaster
  when update $ do
    replSockets <- getReplicas
    socketList <- liftIO $ readTVarIO replSockets
    let encodedCommand = encodeArray True command
    currOffset <- getReplicaSentOffset
    setReplicaSentOffset (BS8.length encodedCommand + currOffset)
    go socketList encodedCommand
    where go [] cmd= pure ()
          go (x:xs) cmd = do
            send x cmd
            go xs cmd

-- TODO: Eventually will need to turn it into a recursive function that will process all the arguments provided, not just 1 key/value pair
setCommand :: MonadStore m => BS.ByteString -> BS.ByteString -> Maybe SetExpiry -> m (Either BS.ByteString Response)
setCommand key val ex = do
  now <- liftIO U.nowNs
  setDataEntry key $ handleExpiry ex now

  let exCommand = case ex of
                   Nothing -> []
                   Just (EX n) -> ["ex", (BS8.pack . show) n]
                   Just (PX n) -> ["px", (BS8.pack . show) n]
  pure $ Right $ Response (encodeSimpleString "OK") (updateReplicas $ ["SET", key, val] ++ exCommand)
  where
    handleExpiry Nothing timeRef = MemoryStoreEntry (MSStringVal val) Nothing
    handleExpiry (Just (EX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (ExpireDuration (fromIntegral $ ex * 1_000_000), ExpireReference timeRef) -- TODO: shouldn't it be multiplied by a 1000??
    handleExpiry (Just (PX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (ExpireDuration (fromIntegral ex), ExpireReference timeRef)

getCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
getCommand key = do
  tv <- getDataEntry key
  case tv of
    Nothing -> pure $ Right $ Response encodeNullBulkString emptyResponse
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> pure $ Right $ Response (encodeBulkString v) emptyResponse
    Just (MemoryStoreEntry (MSStringVal v) (Just (ExpireDuration exDur, ExpireReference exRef))) -> do
      hasPassed <- liftIO $ U.hasElapsedSince exDur exRef
      if hasPassed
        then do
          delDataEntry key
          pure $ Right $ Response encodeNullBulkString emptyResponse
        else pure $ Right $ Response (encodeBulkString v) emptyResponse

pushCommand :: MonadStore m => BS.ByteString -> [BS.ByteString] -> PushCommand -> m (Either BS.ByteString Response)
pushCommand key values pushType = do
  val <- getDataEntry key
  let valuesCount = length values
  let pushArgs = case pushType of
                   RightPushCmd -> "RPUSH"
                   LeftPushCmd  -> "LPUSH"
  case val of
    Nothing -> do
      setDataEntry key (MemoryStoreEntry (MSListVal $ newItems pushType) Nothing)
      pure $ Right $ Response (encodeInteger valuesCount) (updateReplicas $ [pushArgs, key] ++ values)
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      setDataEntry key (MemoryStoreEntry (MSListVal $ newList pushType vs) Nothing)
      pure $ Right $ Response (encodeInteger (length vs + valuesCount)) (updateReplicas $ [pushArgs, key] ++ values)
  where
    newItems RightPushCmd = values
    newItems LeftPushCmd = reverse values
    newList RightPushCmd oldList = oldList ++ values
    newList LeftPushCmd oldList = reverse values ++ oldList

lrangeCommand :: BS.ByteString -> Int -> Int -> ClientApp (Either BS.ByteString Response)
lrangeCommand key start stop = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ Right $ Response (encodeArray True []) emptyResponse
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      pure $ Right $ Response (encodeArray True $ go vs (normStart start) (normStop stop)) emptyResponse
      where
        itemCount = fromIntegral $ length vs
        go :: [BS.ByteString] -> Int -> Int -> [BS.ByteString]
        go xs from to
          | start >= itemCount || from > to = []
          | otherwise = (take (to - from + 1) . drop from) xs
        normStart index
          | index >= 0 = index
          | -index > itemCount = 0
          | otherwise = itemCount + index
        normStop index
          | index >= 0 = (if index >= itemCount then itemCount - 1 else index)
          | -index > itemCount = 0
          | otherwise = itemCount + index

llenCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
llenCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ Right $ Response (encodeInteger 0) emptyResponse
    Just (MemoryStoreEntry (MSListVal v) Nothing) -> pure $ Right $ Response (encodeInteger $ length v) emptyResponse

applyLPopHelper :: MonadStore m => BS.ByteString -> Int -> m (Either BS.ByteString Response)
applyLPopHelper key count = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ Right $ Response encodeNullBulkString (updateReplicas ["LPOP", key, (BS8.pack . show) count])
    Just (MemoryStoreEntry (MSListVal v) Nothing) ->
      let normCount = min count $ length v
      in go v normCount
         where
           go [] _ = pure $ Right $ Response encodeNullBulkString (updateReplicas ["LPOP", key, (BS8.pack . show) count])
           go xs c = do
             setDataEntry key (MemoryStoreEntry (MSListVal (drop c xs)) Nothing)
             pure $ Right $ Response (getPopped c v) (updateReplicas ["LPOP", key, (BS8.pack . show) count])
             where
               getPopped 1 (x : _) = encodeBulkString x
               getPopped popCount xs = encodeArray True (take popCount xs)

lpopCommand :: BS.ByteString -> Int -> ClientApp (Either BS.ByteString Response)
lpopCommand = applyLPopHelper

execWithTimeout :: Double -> ClientApp (Either BS.ByteString Response) -> ClientApp (Either BS.ByteString (Bool, Response)) -> ClientApp (Either BS.ByteString Response)
execWithTimeout timeout timeoutOp otherOp = go 0
  where
    go :: Double -> ClientApp (Either BS.ByteString Response)
    go elapsed
      | elapsed > timeout && timeout > 0 = timeoutOp
      | otherwise = do
          otherOp >>= \case
             Right (shouldContinue, response) -> if shouldContinue then do liftIO $ threadDelay 1_000; go $ elapsed + 0.001 else pure $ Right response

blpopCommand :: BS.ByteString -> Double -> Int -> ClientApp (Either BS.ByteString Response)
blpopCommand key timeout clientID = do
  liftIO $ hPutStrLn stderr ("Starting BLPop with timeout: " <> show timeout)
  execWithTimeout timeout noTimeOp mainOp
  where noTimeOp = pure $ Right $ Response encodeNullArray emptyResponse
        mainOp :: ClientApp (Either BS.ByteString (Bool, Response)) 
        mainOp = do
          val <- getDataEntry key
          case val of
            Nothing -> do
              addWaiterOnce key clientID
              pure $ Right (True, Response "" (pure ()))
            Just (MemoryStoreEntry (MSListVal []) Nothing) -> do
              addWaiterOnce key clientID
              pure $ Right (True, Response "" (pure ()))
            Just (MemoryStoreEntry (MSListVal (x : xs)) Nothing) -> do
              waiters <- getWaiterEntry key
              case waiters of
                Nothing -> do
                  applyLPopHelper key 1 >>= \case
                    Right (Response resp _) -> pure $ Right (False, Response (encodeArray False [encodeBulkString key, resp]) emptyResponse)
                    Left _                  -> pure $ Left "BLPop: An error has occurred"
                Just waitersList -> do
                  if IS.member clientID waitersList
                    then do
                      applyLPopHelper key 1 >>= \case
                         Right (Response resp _) -> delWaiterEntry key >> pure (Right (False, Response (encodeArray False [encodeBulkString key, resp]) emptyResponse))
                    else do
                      addWaiterOnce key clientID
                      pure $ Right (True, Response "" (pure ()))

typeCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
typeCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      (Streams streams) <- getStreams
      case HM.lookup key streams of
        Just _ -> pure $ Right $ Response (encodeSimpleString "stream") emptyResponse
        Nothing -> pure $ Right $ Response (encodeSimpleString "none") emptyResponse
    Just (MemoryStoreEntry (MSStringVal _) _) -> pure $ Right $ Response (encodeSimpleString "string") emptyResponse
    Just (MemoryStoreEntry (MSListVal _) _) -> pure $ Right $ Response (encodeSimpleString "list") emptyResponse

xaddCommand :: MonadStore m => BS.ByteString -> EntryId -> RedisStreamValues -> m (Either BS.ByteString Response)
xaddCommand streamID (EntryId 0 0) values = pure $ Right $ Response (encodeSimpleError "ERR" "The ID specified in XADD must be greater than 0-0") emptyResponse
xaddCommand streamID EntryGenNew values = liftIO U.nowNs >>= \now -> xaddCommand streamID (EntryGenSeq (fromIntegral now)) values
xaddCommand streamID (EntryGenSeq mili) values = do
  (filteredStream, _, _) <- getStream streamID (M.lookupMax . M.filterWithKey (\(EntryId m _) _ -> m == mili))
  case filteredStream of
    Nothing -> xaddCommand streamID (EntryId mili (if mili == 0 then 1 else 0)) values
    Just (EntryId m v, _) -> xaddCommand streamID (EntryId mili (v + 1)) values
xaddCommand streamID entryID@(EntryId mili seq) values = do
  (filteredStream, Stream oldStream, streams) <- getStream streamID (M.lookupMax . M.filterWithKey (\_ _ -> True))
  let replicaRep = ["XADD", streamID, U.entryIdToBS entryID] ++ valuesToArray values []
  case filteredStream of
    Nothing -> addNewEntry M.empty streams
    Just (x, _) ->
      if x >= entryID
        then pure $ Right $ Response (encodeSimpleError "ERR" "The ID specified in XADD is equal or smaller than the target stream top item") (updateReplicas replicaRep)
        else addNewEntry oldStream streams
  where
    addNewEntry :: MonadStore m => M.Map EntryId RedisStreamValues -> RedisStreams -> m (Either BS.ByteString Response)
    addNewEntry oldStream (Streams streams) = do
      let newEntry = values
      let newStream = Stream (M.insert entryID newEntry oldStream)
      let newStreams = HM.insert streamID newStream streams
      setStreams (MemoryStoreEntry (MSStreams (Streams newStreams)) Nothing)
      pure $ Right $ Response (encodeBulkString (U.entryIdToBS entryID)) emptyResponse
    valuesToArray [] acc = acc
    valuesToArray ((key,value):xs) acc = valuesToArray xs (acc ++ [key, value])

xrangeEndHelper :: BS.ByteString -> (EntryId -> RedisStreamValues -> Bool) -> ClientApp (Maybe RangeEntryId)
xrangeEndHelper key f = do
  (filteredStream, _, streams) <- getStream key (M.lookupMax . M.filterWithKey f)
  case filteredStream of
    Nothing -> pure Nothing
    Just (EntryId m v, _) -> pure $ Just (RangeEntryId m v)

xrangeHelper :: BS.ByteString -> (EntryId -> EntryId -> Bool) -> RangeEntryId -> RangeEntryId -> ClientApp (Either BS.ByteString Response)
xrangeHelper key rangef (RangeEntryId mili1 mili2) (RangeEntryId seq1 seq2) = do
  (_, Stream oldStream, _) <- getStream key (const Nothing . M.filterWithKey (\_ _ -> True))
  let allKeysValues = M.toAscList (U.range rangef (EntryId mili1 mili2) (EntryId seq1 seq2) oldStream)
  let resp = parseKeysValues allKeysValues
  pure $ Right $ Response resp emptyResponse
  where
    parseKeysValues :: [(EntryId, RedisStreamValues)] -> BS.ByteString
    parseKeysValues keysValues =
      "*" <> (BS8.pack . show . length) keysValues <> "\r\n"
        <> foldr (\(id, valuesMap) acc -> "*2\r\n" <> encodeBulkString (U.entryIdToBS id) <> convertMap valuesMap <> acc) BS.empty keysValues
    convertMap :: RedisStreamValues -> BS.ByteString
    convertMap l =
      "*"
        <> (BS8.pack . show . (* 2) . length) l <> "\r\n"
        <> foldr (\(k, v) acc -> encodeBulkString k <> encodeBulkString v <> acc) BS.empty l

xrangeCommand :: BS.ByteString -> RangeEntryId -> RangeEntryId -> ClientApp (Either BS.ByteString Response)
xrangeCommand key mili RangeMinusPlus = do
  res <- xrangeEndHelper key (\_ _ -> True)
  case res of
    Nothing -> pure $ Right $ Response encodeNullArray emptyResponse
    Just seq -> xrangeCommand key mili seq
xrangeCommand key RangeMinusPlus seq = do
  (filteredStream, Stream oldStream, streams) <- getStream key (M.lookupMin . M.filterWithKey (\_ _ -> True))
  case filteredStream of
    Nothing -> pure $ Right $ Response encodeNullArray emptyResponse
    Just (EntryId m v, _) -> xrangeCommand key (RangeEntryId m v) seq
xrangeCommand key (RangeMili mili) seq@(RangeEntryId seq1 seq2) = xrangeCommand key (RangeEntryId mili 0) seq
xrangeCommand key mili@(RangeEntryId _ _) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ Right $ Response (encodeSimpleError "ERR" "XRANGE: The end id does not exist") emptyResponse
    Just (RangeEntryId _ newEnd) -> xrangeCommand key mili (RangeEntryId seq newEnd)
xrangeCommand key (RangeMili mili) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ Right $ Response (encodeSimpleError "ERR" "XRANGE: The end id does not exist") emptyResponse
    Just (RangeEntryId _ newEnd) -> xrangeCommand key (RangeEntryId mili 0) (RangeEntryId seq newEnd)
xrangeCommand key mili@(RangeEntryId _ _) seq@(RangeEntryId _ _) = xrangeHelper key (<) mili seq

xreadEntriesAvailable :: [(BS.ByteString, RangeEntryId)] -> ClientApp (Bool, [(BS.ByteString, RangeEntryId)])
xreadEntriesAvailable keyIds = do
  go keyIds []
  where
    go :: [(BS.ByteString, RangeEntryId)] -> [(BS.ByteString, RangeEntryId)] -> ClientApp (Bool, [(BS.ByteString, RangeEntryId)])
    go [] newIds = pure (False, newIds)
    go wholelist@(entry@(key, entry_id) : xs) newIds = do
      res <- xrangeEndHelper key (\_ _ -> True)
      case res of
        Nothing -> case entry_id of
          RangeDollar -> go xs (newIds ++ [(key, RangeEntryId 0 0)])
          _ -> go xs (newIds ++ [entry])
        Just r@(RangeEntryId endMili endSeq) -> do
          (_, Stream oldStream, _) <- getStream key (const Nothing . M.filterWithKey (\_ _ -> True))
          getRange entry_id endMili endSeq oldStream
          where
            getRange :: RangeEntryId -> Word64 -> Word64 -> M.Map EntryId RedisStreamValues -> ClientApp (Bool, [(BS.ByteString, RangeEntryId)])
            getRange (RangeEntryId startMili startSeq) endMili endSeq oldStream = do
              let allKeysValues = U.range (<=) (EntryId startMili startSeq) (EntryId endMili endSeq) oldStream
              if null allKeysValues then go xs (newIds ++ [entry]) else pure (True, newIds ++ [entry])
            getRange RangeDollar endMili endSeq oldStream = do
              let updatedEntry = (key, RangeEntryId endMili endSeq)
              go xs (newIds ++ [updatedEntry])

xreadCommand :: [(BS.ByteString, RangeEntryId)] -> Maybe Double -> ClientApp (Either BS.ByteString Response)
xreadCommand keysIds (Just timeout) = go 0 keysIds
  where
    go elapsed ids = do
      (hasEntries, newIds) <- xreadEntriesAvailable ids
      if hasEntries
        then xreadCommand newIds Nothing
        else
          if elapsed > (timeout / 1_000) && timeout > 0
            then pure $ Right $ Response encodeNullArray emptyResponse
            else do
              liftIO $ threadDelay 1_000
              go (elapsed + 0.001) newIds
xreadCommand keysIds Nothing = do
  result <- go keysIds BS.empty 0
  pure $ Right $ Response ("*" <> (BS8.pack . show . length) keysIds <> "\r\n" <> result) emptyResponse
  where
    go :: [(BS.ByteString, RangeEntryId)] -> BS.ByteString -> Double -> ClientApp BS.ByteString
    go [] acc elapsed = pure acc
    go ((stream_id, entry_id) : xs) acc elapsed = do
      res <- xrangeEndHelper stream_id (\_ _ -> True)
      case res of
        Nothing -> pure encodeNullArray
        Just seq -> do
          eitherResp <- xrangeHelper stream_id (<=) entry_id seq
          case eitherResp of
            Right (Response streamResp _) -> go xs (acc <> "*2\r\n" <> encodeBulkString stream_id <> streamResp) elapsed
            Left _ -> go xs acc elapsed

incrCommand :: MonadStore m => BS.ByteString -> m (Either BS.ByteString Response)
incrCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      setDataEntry key (MemoryStoreEntry (MSStringVal "1") Nothing)
      pure $ Right $ Response (encodeInteger 1) (updateReplicas ["INCR", key])
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> case U.bsToInt v of
      Nothing -> pure $ Right $ Response (encodeSimpleError "ERR" "value is not an integer or out of range") emptyResponse
      Just i -> do
        setDataEntry key (MemoryStoreEntry (MSStringVal $ (BS8.pack . show) (i + 1)) Nothing)
        pure $ Right $ Response (encodeInteger $ i + 1) (updateReplicas ["INCR", key])

execCommand :: ClientApp (Either BS.ByteString Response)
execCommand = do
  multi <- getMulti
  updateMulti False
  if multi
  then do
    ml <- getMultiList
    if null ml
    then pure $ Right $ Response (encodeArray True []) emptyResponse
    else do
      ml <- getMultiList
      res <- go ml []
      pure $ Right $ Response (encodeArray False res) emptyResponse
  else pure $ Right $ Response (encodeSimpleError "ERR" "EXEC without MULTI") emptyResponse
  where go :: [ClientApp (Either BS.ByteString Response)] -> [BS.ByteString] -> ClientApp [BS.ByteString]
        go [] acc = pure acc
        go (x:xs) acc = do
           eitherResp <- x
           case eitherResp of
                Right (Response resp _) -> go xs (acc ++ [resp])
                Left e -> go xs acc

discardCommand :: ClientApp (Either BS.ByteString Response)
discardCommand = do
  multi <- getMulti
  if multi
  then do
    updateMulti False
    resetMultiCommands
    pure $ Right $ Response (encodeSimpleString "OK") emptyResponse
  else pure $ Right $ Response (encodeSimpleError "ERR" "DISCARD without MULTI") emptyResponse

handleMultiCmd :: ClientApp (Either BS.ByteString Response) -> ClientApp (Either BS.ByteString Response)
handleMultiCmd op = do
  multi <- getMulti
  if multi
  then do
    addMultiCommand op
    pure $ Right $ Response (encodeSimpleString "QUEUED") emptyResponse
  else op

infoCommand :: InfoRequest -> ClientApp (Either BS.ByteString Response)
infoCommand Replication = do
  role <- getRole
  case role of
    Master repID repOffset -> pure $ Right $ Response (masterResponse repID repOffset) emptyResponse
    Slave roHost roPort -> pure $ Right $ Response (encodeBulkString "# Replication\nrole:slave") emptyResponse
  where masterResponse rID repOS = encodeBulkString $ "# Replication\nrole:master\nmaster_replid:" <> BS8.pack rID <> "\nmaster_repl_offset:" <> (BS8.pack . show) repOS

replConfCommand :: ReplConfOptions -> ClientApp (Either BS.ByteString Response)
replConfCommand (ListeningPort port) = do
  pure $ Right $ Response (encodeSimpleString "OK") emptyResponse
replConfCommand (Capa capa) = do
  pure $ Right $ Response (encodeSimpleString "OK") emptyResponse
replConfCommand (AckWith offset) = do
  serverOffset <- getReplicaSentOffset
  if offset == serverOffset
  then do
    tvComplete <- asks $ senvCompleteReplicaCount . cenvShared
    liftIO . atomically $ do
      current <- readTVar tvComplete
      writeTVar tvComplete $ current + 1
    current <- liftIO $ readTVarIO tvComplete
    pure $ Right $ Response mempty emptyResponse
  else pure $ Right $ Response mempty emptyResponse

sendSnapshot :: ClientApp ()
sendSnapshot = do
  socket <- getSocket
  case U.decodeRdbBase64 U.emptyRdbFile of
    Right x -> liftIO $ send socket $ encodeRdbFile x

psyncCommand :: PSyncRequest -> ClientApp (Either BS.ByteString Response)
psyncCommand PSyncUnknown = do
  repl <- getClientReplication
  case repl of
    Master repID _ -> do
      _sock <- getSocket
      addReplica _sock
      pure $ Right $ Response (encodeSimpleString $ "FULLRESYNC " <> BS8.pack repID <> " 0") sendSnapshot
    _ -> pure $ Right $ Response mempty emptyResponse -- A slave will never get a PSync command from a client

sendAckCommand :: Socket -> Int -> TVar Int -> IO ()
sendAckCommand sock serverOffset tvCompleted = do
  SA.withAsync worker $ \a -> do
    SA.wait a
  where
       worker :: IO ()
       worker = do
          -- liftIO $ hPutStrLn stderr "Sending REPLCONF GETACK *"
          liftIO $ send sock $ encodeArray True ["REPLCONF", "GETACK", "*"]

waitCommand :: Int -> Double -> ClientApp (Either BS.ByteString Response)
waitCommand reqReady timeout = do
  tv <- getReplicas
  replicas <- liftIO $ readTVarIO tv
  serverOffset <- getReplicaSentOffset
  tvComplete <- asks (senvCompleteReplicaCount . cenvShared)
  liftIO . atomically $ modifyTVar' tvComplete (const 0)

  -- liftIO $ hPutStrLn stderr ("SERVER OFFSET " <> show serverOffset)
  sendAcknowledgements serverOffset tvComplete replicas
  currOffset <- getReplicaSentOffset
  if currOffset > 0
  then do
    liftIO $ hPutStrLn stderr "Sent ACK to all clients"
    execWithTimeout (timeout/1_000) countSoFarOp countFullOp
  else pure $ Right $ Response (encodeInteger $ length replicas) emptyResponse

  where sendAcknowledgements serverOS tvComplete [] = pure ()
        sendAcknowledgements serverOS tvComplete (x:xs) = do
          liftIO $ sendAckCommand x serverOS tvComplete
          sendAcknowledgements serverOS tvComplete xs
        countSoFarOp :: ClientApp (Either BS.ByteString Response)
        countSoFarOp = do
          result <- getCompleteReplicas
          -- liftIO $ hPutStrLn stderr ("WAIT Timedout, returning " <> show result)
          updateServerSent
          pure $ Right $ Response (encodeInteger result) emptyResponse
        countFullOp :: ClientApp (Either BS.ByteString (Bool, Response))
        countFullOp = do
          result <- getCompleteReplicas
          if result >= reqReady
          then do
            -- liftIO $ hPutStrLn stderr ("WAIT got enough requests, returning " <> show result)
            updateServerSent
            pure $ Right (False, Response (encodeInteger result) emptyResponse)
          else pure $ Right (True, Response "" emptyResponse)
        getCompleteReplicas = do
          tv <- asks (senvCompleteReplicaCount . cenvShared)
          liftIO $ readTVarIO tv
        updateServerSent = do
            let encodedCommand = encodeArray True ["REPLCONF", "GETACK", "*"]
            currOffset <- getReplicaSentOffset
            setReplicaSentOffset (BS8.length encodedCommand + currOffset)

configCommand :: ConfigArgs -> ClientApp (Either BS.ByteString Response)
configCommand ConfigGetDir = do
  dir <- asks $ cfgDir . ccfgShared . cenvConfig
  pure $ Right $ Response (encodeArray True ["dir", BS8.pack dir]) emptyResponse
configCommand ConfigGetFileName = do
  fileName <- asks $ cfgRDBFileName . ccfgShared . cenvConfig
  pure $ Right $ Response (encodeArray True ["dbfilename", BS8.pack fileName]) emptyResponse

-- TODO: Filtering is VERY inefficient, redo at some point
keysCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
keysCommand "*" = do
  tvStore <- getData
  store <- liftIO $ readTVarIO tvStore
  let result = M.keys store
  pure $ Right $ Response (encodeArray True result) emptyResponse
keysCommand filter = do
  tvStore <- getData
  store <- liftIO $ readTVarIO tvStore
  let result = M.keys store
  pure $ Right $ Response (encodeArray True (foldl' foldme [] result)) emptyResponse
  where
    foldme :: [BS.ByteString] -> BS.ByteString -> [BS.ByteString]
    foldme acc item = if go (BS8.unpack filter) (BS8.unpack item)
                      then acc ++ [BS8.pack (BS8.unpack item)]
                      else acc
    go :: String -> String -> Bool
    go [] [] = True
    go [] _ = False
    go ('*':_) [] = True
    go (x:_) [] = False
    go (x:xs) (y:ys)
      | x == '*' = True
      | x /= y = False
      | otherwise = go xs ys

subscribeCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
subscribeCommand channel = do
  socket <- getSocket
  setSubscribed True
  addSubChannel channel
  currChannels <- getSubChannels
  addChannelSubcriber channel socket
  pure $ Right $ Response (encodeArray False [encodeBulkString "subscribe", encodeBulkString channel, encodeInteger $ length currChannels]) emptyResponse

publishCommand :: BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
publishCommand channel msg = do
  socket <- getSocket
  clientList <- getChannelClients channel
  sendMessage clientList
  pure $ Right $ Response (encodeInteger $ length clientList) emptyResponse
  where sendMessage [] = pure ()
        sendMessage (x:xs) = do
          send x $ encodeArray True ["message", channel, msg]
          sendMessage xs

zaddCommand :: BS.ByteString -> Double -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
zaddCommand name score member = do
  oldCount <- getZSetMemberCount name member
  addMemberToZSet name score member
  currCount <- getZSetMemberCount name member
  pure $ Right $ Response (encodeInteger $ currCount - oldCount) emptyResponse

zrankCommand :: BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
zrankCommand name member = do
  (ZSet scoreMap memberDict) <- getZSet name
  maybeVal <- runMaybeT $ do
    score <- MaybeT . pure $ HM.lookup member memberDict
    let precedingSets = M.elems (fst (M.split score scoreMap))
    let precedingCount = (sum . map S.size) precedingSets
    memberSet <- MaybeT . pure $ M.lookup score scoreMap
    rank <- MaybeT . pure  $ S.lookupIndex member memberSet
    pure $ Response (encodeInteger (precedingCount + rank)) emptyResponse
  case maybeVal of
    Just result -> pure $ Right result
    Nothing  -> pure $ Right $ Response encodeNullBulkString emptyResponse

zrangeCommand :: BS.ByteString -> Int -> Int -> ClientApp (Either BS.ByteString Response)
zrangeCommand name start end = do
  (ZSet scoreMap _) <- getZSet name
  let allScores = M.elems scoreMap
  let allScoresList = concatMap S.toAscList allScores
  let itemCount = length allScoresList
  let nStart = normStart start itemCount
  let nEnd = normStop end itemCount
  pure $ Right $ Response (encodeArray True (take (nEnd - nStart + 1) $ drop nStart allScoresList)) emptyResponse
  where normStart index itemCount
          | index >= 0 = index
          | -index > itemCount = 0
          | otherwise = itemCount + index
        normStop index itemCount
          | index >= 0 = if index >= itemCount then itemCount - 1 else index
          | -index > itemCount = 0
          | otherwise = itemCount + index

zcardCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
zcardCommand name = do
  (ZSet scoreMap _) <- getZSet name
  pure $ Right $ Response (encodeInteger $ (sum . map S.size) $ M.elems scoreMap) emptyResponse

zscoreCommand :: BS.ByteString -> BS.ByteString  -> ClientApp (Either BS.ByteString Response)
zscoreCommand name member = do
  (ZSet _ memberDict) <- getZSet name
  case HM.lookup member memberDict of
    Just score -> pure $ Right $ Response (encodeBulkString (BS8.pack $ show score)) emptyResponse
    Nothing    -> pure $ Right (Response encodeNullBulkString emptyResponse)

zremCommand :: BS.ByteString -> BS.ByteString  -> ClientApp (Either BS.ByteString Response)
zremCommand name member = do
  (ZSet scoreMap memberDict) <- getZSet name
  case HM.lookup member memberDict of
    Just score -> let updatedMemberDict = HM.delete member memberDict
                  in case M.lookup score scoreMap of
                        Just memberSet -> do
                          let newMap = M.alter (Just . S.delete member . fromMaybe S.empty) score scoreMap
                          tv <- getZSets
                          liftIO . atomically $
                            modifyTVar' tv $ HM.alter (Just . const (ZSet newMap updatedMemberDict) . fromMaybe (ZSet M.empty HM.empty)) name
                          pure $ Right $ Response (encodeInteger 1) emptyResponse
                        Nothing        -> pure $ Right $ Response (encodeInteger 0) emptyResponse
    Nothing    -> pure $ Right $ Response (encodeInteger 0) emptyResponse

geoAddCommand :: BS.ByteString -> Double -> Double -> BS.ByteString  -> ClientApp (Either BS.ByteString Response)
geoAddCommand name longitude latitude member = do
  if longitude >= 180 || longitude <= -180
  then pure $ Right $ Response (encodeSimpleError "ERR" "longitude should be between -180.0 and 180.0 degrees") emptyResponse
  else if latitude >= 85.05112878 || latitude <= -85.05112878
       then pure $ Right $ Response (encodeSimpleError "ERR" "latitude should be between -85.05112878 and 85.05112878 degrees") emptyResponse
       else do
         zaddCommand name (U.interleaveGeo latitude longitude) member >>= \case
            Right (Response resp _) -> pure $ Right $ Response (encodeInteger 1) emptyResponse
            Left _                  -> pure $ Left "Error in GeoAdd Command"

geoPosCommand :: BS.ByteString -> [BS.ByteString]  -> ClientApp (Either BS.ByteString Response)
geoPosCommand name members = do
  (ZSet _ memberDict) <- getZSet name
  let values = map (`HM.lookup` memberDict) members
  pure $ Right $ Response (encodeArray False (go values [])) emptyResponse
  where
    go :: [Maybe Double] -> [BS.ByteString] -> [BS.ByteString]
    go [] acc = acc
    go (x:xs) acc = do
      case x of
        Just score -> let (lat, long) = U.deinterleaveGeo score
                      in go xs $ acc ++ [encodeArray True [(BS8.pack . show) long, (BS8.pack . show) lat]]
        Nothing    -> go xs $ acc ++ [encodeNullArray]

geoDistCommand :: BS.ByteString -> BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
geoDistCommand name member1 member2 = do
  (ZSet _ memberDict) <- getZSet name
  result <- runMaybeT $ do
    score1 <- MaybeT . pure $ HM.lookup member1 memberDict
    score2 <- MaybeT . pure $ HM.lookup member2 memberDict
    let (lat1, long1) = U.deinterleaveGeo score1
    let (lat2, long2) = U.deinterleaveGeo score2
    pure $ U.calcGeoDistance (long1, lat1) (long2, lat2)
  case result of
    Just dist -> pure $ Right $ Response (encodeBulkString ((BS8.pack . show) dist)) emptyResponse
    Nothing   -> pure $ Right $ Response (encodeSimpleError "ERR" "GeoDist: one of the members does not exist") emptyResponse

geoSearchCommand :: BS.ByteString -> Double -> Double -> Double -> DistUnit -> ClientApp (Either BS.ByteString Response)
geoSearchCommand name longitude latitude radius unit = do
  (ZSet _ memberDict) <- getZSet name
  let normRadius = case unit of
                     DistKilometer -> radius * 1_000
                     DistMile      -> radius * 1609.344
                     _             -> radius
  let membersInRange = HM.foldrWithKey' (\member score acc -> if calcDistance score longitude latitude <= normRadius then acc ++ [member] else acc) [] memberDict
  pure $ Right $ Response (encodeArray True membersInRange) emptyResponse
  where calcDistance score centerLong centerLat = let (lat1, long1) = U.deinterleaveGeo score
                                                  in U.calcGeoDistance (long1, lat1) (centerLong, centerLat)

aclCommand :: AclSubCmd -> ClientApp (Either BS.ByteString Response)
aclCommand opt =
  case opt of
    AclWhoAmI -> do
      UserData {name = n} <- gets userData
      pure $ Right $ Response (encodeBulkString n) emptyResponse
    AclGetUser user -> do
      UserData {flags = fl, passwords = ps} <- gets userData
      let response = [encodeBulkString "flags", encodeArray True fl, encodeBulkString "passwords", encodeArray True ps]
      pure $ Right $ Response (encodeArray False response) emptyResponse
    AclSetUser user password -> do
      UserData {flags = fl, passwords = ps} <- gets userData
      let newFlags = delete "nopass" fl
      let encryptedPass = (B16.encode . SHA256.hash) password
      let newPasswords = encryptedPass : ps

      -- this client is now authcenticated
      modify' (\cs -> cs {isAuth = True})

      -- Update this server now requires authentication
      tv <- asks $ senvIsAuth . cenvShared
      liftIO . atomically $
        modifyTVar' tv (const True)
      tvUserPass <- asks $ senvAuthUsers . cenvShared
      liftIO . atomically $
        modifyTVar' tvUserPass (HM.insert user encryptedPass)

      modify' (\cs -> cs { userData = UserData {name = user, passwords = newPasswords, flags = newFlags }})
      pure $ Right $ Response (encodeSimpleString "OK") emptyResponse

authCommand :: BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
authCommand userName password = do
  let encodedPass = (B16.encode . SHA256.hash) password
  UserData {name = user, flags = fl, passwords = ps} <- gets userData
  tvUserPass <- asks $ senvAuthUsers . cenvShared
  userPassMap <- liftIO $ readTVarIO tvUserPass
  tvServerAuth <- asks $ senvIsAuth . cenvShared
  isServerAuth <- liftIO $ readTVarIO tvServerAuth
  if isServerAuth then
    case HM.lookup userName userPassMap of
      Just serverPass ->
        if serverPass == encodedPass
        then do
          let newPasswords = ps ++ [encodedPass]
          modify' (\cs -> cs { isAuth = True, userData = UserData { name = user, flags = fl, passwords = newPasswords }})

          pure $ Right $ Response (encodeSimpleString "OK") emptyResponse
        else pure $ Right $ Response (encodeSimpleError "WRONGPASS" "invalid username-password pair or user is disabled.") emptyResponse
      Nothing -> pure $ Right $ Response (encodeSimpleError "ERR" "Server is authenticated but user not found.") emptyResponse
  else do
    if user == userName && elem encodedPass ps
    then do
      modify' (\cs -> cs {isAuth = True}) -- this client is now authcenticated
      pure $ Right $ Response (encodeSimpleString "OK") emptyResponse
    else do
      pure $ Right $ Response (encodeSimpleError "WRONGPASS" "invalid username-password pair or user is disabled.") emptyResponse

approvedSubCommand :: Command -> Bool
approvedSubCommand cmd = case cmd of
                           Subscribe {}   -> True
                           Ping           -> True
                           Publish {}     -> True
                           Unsubscribe {} -> True
                           _              -> False

execSubCommand :: Command -> ClientApp ()
execSubCommand cmd = do
  socket <- getSocket
  case cmd of
    (Subscribe channel) -> do
       eitherResp <- subscribeCommand channel
       case eitherResp of
         Right (Response resp _) -> liftIO $ send socket resp
         Left _ -> pure ()
    Ping                -> liftIO $ send socket $ encodeArray True ["pong", ""]
    Publish channel msg -> do
      eitherResp <- publishCommand channel msg
      case eitherResp of
        Right (Response resp _) -> liftIO $ send socket resp
        Left _                  -> pure ()
    Unsubscribe channel -> do
      socket <- getSocket
      setSubscribed False
      removeSubChannel channel
      removeChannelSubscriber channel socket
      remaining <- getSubChannels
      send socket $ encodeArray False [encodeBulkString "unsubscribe", encodeBulkString channel, encodeInteger (S.size remaining)]

-- TODO: refactor the need for this function
getCommandName :: Command -> BS.ByteString
getCommandName cmd = case cmd of
                       Echo {}     -> "Echo"
                       Set {}      -> "Set"
                       Get {}      -> "Get"
                       RPush {}    -> "RPush"
                       LPush {}    -> "LPush"
                       LRange {}   -> "LRange"
                       LLen {}     -> "LLen"
                       LPop {}     -> "LPop"
                       BLPop {}    -> "BLPop"
                       Type {}     -> "Type"
                       XAdd {}     -> "XAdd"
                       XRange {}   -> "XRange"
                       XRead {}    -> "XRead"
                       Incr {}     -> "Incr"
                       Multi       -> "Multi"
                       Exec        -> "Exec"
                       Discard     -> "Discard"
                       Info {}     -> "Info"
                       ReplConf {} -> "ReplConf"
                       Psync {}    -> "Psync"
                       Wait {}     -> "Wait"
                       Config {}   -> "Config"
                       Keys {}     -> "Keys"
                       ZAdd {}     -> "ZAdd"
                       ZRank {}    -> "ZRank"
                       ZRange {}   -> "ZRange"
                       ZCard {}    -> "ZCard"

isAuthCommand :: Command -> ClientApp Bool
isAuthCommand = \case Auth {} -> pure True; _ -> pure False

handleConnection :: ClientApp ()
handleConnection = go ""
  where
    go acc = do
      sock <- getSocket
      clientID <- getClientID
      mb <- liftIO $ recv sock 4096
      case mb of
        Nothing -> pure ()
        Just buf -> do
          -- liftIO $ hPutStrLn stderr ("Received: " <> BS8.unpack buf)
          case parseOneCommand buf of
            RParserNeedMore -> go (acc <> buf)
            -- keep partial bytes for next recv
            RParsed cmd rest -> do
              tvServerAuth <- asks $ senvIsAuth . cenvShared
              isServerAuth <- liftIO $ readTVarIO tvServerAuth
              isUserAuth <- gets isAuth
              isAuthCommand <- isAuthCommand cmd
              if (isServerAuth && (isUserAuth || isAuthCommand)) || not isServerAuth then do
                subMode <- getSubscribed
                if subMode then do
                   if approvedSubCommand cmd then execSubCommand cmd
                   else do
                     let commandName = getCommandName cmd
                     liftIO $ send sock $ encodeSimpleError "ERR" ("Can't execute '" <> commandName <> "' when one or more subscriptions exist")
                else do
                  eitherResp <- case cmd of
                     Ping -> handleMultiCmd $ pure $ Right $ Response (encodeSimpleString "PONG") emptyResponse
                     (Echo str) -> handleMultiCmd $ pure $ Right $ Response (encodeBulkString str) emptyResponse
                     (Set key val args) -> handleMultiCmd $ setCommand key val args
                     (Get key) -> handleMultiCmd $ getCommand key
                     (RPush key values) -> handleMultiCmd $ pushCommand key values RightPushCmd
                     (LPush key values) -> handleMultiCmd $ pushCommand key values LeftPushCmd
                     (LRange key start stop) -> handleMultiCmd $ lrangeCommand key start stop
                     (LLen key) -> handleMultiCmd $ llenCommand key
                     (LPop key count) -> handleMultiCmd $ lpopCommand key count
                     (BLPop key timeout) -> handleMultiCmd $ blpopCommand key timeout clientID
                     (Type key) -> handleMultiCmd $ typeCommand key
                     (XAdd streamID entryID values) -> handleMultiCmd $ xaddCommand streamID entryID values
                     (XRange key start end) -> handleMultiCmd $ xrangeCommand key start end
                     (XRead keysIds timeout) -> handleMultiCmd $ xreadCommand keysIds timeout
                     (Incr key) -> handleMultiCmd $ incrCommand key
                     Multi -> do
                       updateMulti True
                       pure $ Right $ Response (encodeSimpleString "OK") emptyResponse
                     Exec -> execCommand
                     Discard -> discardCommand
                     (Info infoRequest) -> handleMultiCmd $ infoCommand infoRequest
                     (ReplConf replOptions) -> replConfCommand replOptions
                     (Psync req) -> psyncCommand req
                     (Wait replicaNum timeout) -> waitCommand replicaNum timeout
                     (Config opt) -> configCommand opt
                     (Keys filter) -> keysCommand filter
                     (Subscribe channel) -> subscribeCommand channel
                     (Publish channel msg) -> publishCommand channel msg
                     (ZAdd name score member) -> zaddCommand name score member
                     (ZRank name member) -> zrankCommand name member
                     (ZRange name start end) -> zrangeCommand name start end
                     (ZCard name) -> zcardCommand name
                     (ZScore name member) -> zscoreCommand name member
                     (ZRem name member) -> zremCommand name member
                     (GeoAdd name longtitude latitude member) -> geoAddCommand name longtitude latitude member
                     (GeoPos name member) -> geoPosCommand name member
                     (GeoDist name member1 member2) -> geoDistCommand name member1 member2
                     (GeoSearch name longitude latitude radius unit) -> geoSearchCommand name longitude latitude radius unit
                     (Auth userName password) -> authCommand userName password
                     (Acl opt) -> aclCommand opt
                     Cmd  -> pure $ Right $ Response encodeEmptyArray emptyResponse -- convenience command for running redis-cli in bulk mode
                  case eitherResp of
                    Right (Response resp nextAction) -> liftIO (send sock resp) >> nextAction
                    Left _ -> pure ()
              else liftIO $ send sock (encodeSimpleError "NOAUTH" "Authentication required.")
              go rest                -- rest may already contain next command
            RParserErr e -> do
              liftIO $ send sock e

---------------------------------------------------------------------------------------------------------
-- Slave functions

recvByParser :: (BS.ByteString -> StringParserResult) -> Socket -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
recvByParser parser sock = go
  where
    go acc = do
      case parser acc of
        SParserFullString str rest -> pure $ Right (str, rest)
        SParserPartialString       -> recv sock 4096 >>= \case
          Nothing -> pure $ Left "Received only partial response"
          Just chunk -> go (acc <> chunk)
        SParserError msg           -> pure $ Left msg

recvSimpleResponse :: Socket -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
recvSimpleResponse = recvByParser parseSimpleString

recvRDBFile :: Socket -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
recvRDBFile = recvByParser parseRDBFile

getAckCommand :: ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
getAckCommand = do
  offset <- getReplicaOffset
  pure $ Right (encodeArray True ["REPLCONF", "ACK", (BS8.pack . show) offset], "")

awaitServerUpdates :: Socket -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
awaitServerUpdates sock = go 0
  where
    go :: Int -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
    go offset buffered = do
      mb <- if BS.null buffered then liftIO (recv sock 4096) else pure (Just buffered)
      case mb of
        Nothing -> pure $ Left "Error (Replica): Connection might have been closed by the server"
        Just buf -> do
          setReplicaOffset offset
          case parseOneCommand buf of
            RParsed cmd rest -> do
              let processedCount = BS.length buf - BS.length rest
              case cmd of
                Set key val args        -> setCommand key val args >>= \case
                  Right (Response r _) -> pure $ Right (r, "")
                  Left _ -> pure $ Left (BS8.pack "Err (Replica): Set command")
                RPush key values        -> pushCommand key values RightPushCmd >>= \case
                  Right (Response r _) -> pure $ Right (r, "")
                  Left _ -> pure $ Left "Err (Replica): RPush command"
                LPush key values        -> pushCommand key values LeftPushCmd >>= \case
                  Right (Response r _) -> pure $ Right (r, "")
                  Left _ -> pure $ Left "Err (Replica): LPush command"
                LPop key count          -> applyLPopHelper key count >>= \case
                  Right (Response r _) -> pure $ Right (r, "")
                  Left _ -> pure $ Left "Err (Replica): LPop command"
                XAdd streamID entryID v -> xaddCommand streamID entryID v >>= \case
                  Right (Response r _) -> pure $ Right (r, "")
                  Left _ -> pure $ Left "Err (Replica): XAdd command"
                Incr key                -> incrCommand key >>= \case
                  Right (Response r _) -> pure $ Right (r, "")
                  Left _ -> pure $ Left "Err (Replica): Incr command"
                ReplConf GetAck         -> do
                  eitherResp <- getAckCommand
                  case eitherResp of
                    Right (resp, _) -> do
                      liftIO (send sock resp)
                      pure $ Right (resp, "")
                    Left _          -> pure $ Left "Error (Replica): unknown command from the server"
                _                       -> pure $ Left "Error (Replica): unknown command from the server"
              go (offset + processedCount) rest
            RParserNeedMore -> do
              mbMore <- liftIO $ recv sock 4096
              case mbMore of
                Nothing   -> pure $ Left "Error (Replica): Incomplete command received and server seemed to have closed the connection"
                Just more -> go offset (buf <> more)
            RParserErr _ -> pure $ Left "Error (Replica): Command received from server coud not be parsed properly"

runReplica :: ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
runReplica = do
  clientPort <- getPort
  getReplication >>= \case
    Master _ _ -> pure $ Right ("", "")
    -- withRunInIO :: ((ReplicaApp () -> IO ()) -> IO) -> ReplicaApp()
    Slave host port -> withRunInIO $ \runInIO -> do
      putStrLn ("Trying to connect to " <> host <> " " <> port)
      _ <- UL.async $ connect host port $ \(_sock, _addr) ->
        -- runInIO :: ReplicaApp () -> IO ()
        runInIO $ do
          -- ExceptT ByteString ClientApp (ByteString, ByteString)
          e <- runExceptT $ do

             liftIO $ send _sock (encodeArray True ["PING"])

             (pongResp, pending0) <- ExceptT $ recvSimpleResponse _sock mempty
  
             unless (pongResp == "PONG") $
               throwError "Replica: did not receive PONG back from the server"
  
             -- same rule for all the recv* functions:
             liftIO $ send _sock (encodeArray True ["REPLCONF", "listening-port", BS8.pack clientPort])
             (replResp1, pending1) <- ExceptT $ recvSimpleResponse _sock pending0
             unless (replResp1 == "OK") $
               throwError "Replica: expected OK after REPLCONF listening-port"
  
             liftIO $ send _sock (encodeArray True ["REPLCONF", "capa", "psync2"])
             (replResp2, pending2) <- ExceptT $ recvSimpleResponse _sock pending1
             unless (replResp2 == "OK") $
               throwError "Replica: expected OK after REPLCONF capa"
  
             liftIO $ send _sock (encodeArray True ["PSYNC", "?", "-1"])
             (_replPsync, pending3) <- ExceptT $ recvSimpleResponse _sock pending2
  
             (_rdbFile, pending4) <- ExceptT $ recvRDBFile _sock pending3
             (resp, _pending5)    <- ExceptT $ awaitServerUpdates _sock pending4
             pure resp

          case e of
             Left err  -> liftIO $ putStrLn ("replication failed: " <> show err)
             Right resp -> liftIO $ print resp
      pure $ Right ("", "")

----------- Slave functions END

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering

  -- You can use print statements as follows for debugging, they'll be visible when running tests.
  -- hPutStrLn stderr "Logs from your program will appear here"

  cfgCli <- parseCli
  store <- newMemoryStore
  nextID <- newTVarIO (0 :: Int)

  let port = fromMaybe "6379" (cliPort cfgCli)
  let dir = fromMaybe "/tmp/redis-files" (cliDir cfgCli)
  let rdbFileName = fromMaybe "dump.rdb" (cliFileName cfgCli)

  CE.try (withBinaryFile (dir </> rdbFileName) ReadMode (readDBFile store))
      :: IO (Either CE.IOException ())

  repID <- U.randomAlphaNum40BS
  let sharedCfg = case cliReplication cfgCli of
              WantMaster -> SharedConfig port dir rdbFileName (Master (BS8.unpack repID) 0)
              WantSlave wantHost wantPort -> SharedConfig port dir rdbFileName (Slave wantHost wantPort)

  newZSets <- newTVarIO HM.empty
  newReplicas <- newTVarIO []
  sentOffset <- newTVarIO 0
  complReplicas <- newTVarIO 0
  newChannels <- newTVarIO HM.empty
  newIsAuth <- newTVarIO False
  newAuthUsers <- newTVarIO HM.empty
  
  let sharedEnv = SharedEnv { senvStore = store
                            , senvSets = newZSets
                            , senvConfig = sharedCfg
                            , senvReplicas = newReplicas
                            , senvReplicaSentOffset = sentOffset
                            , senvCompleteReplicaCount = complReplicas
                            , senvChannels = newChannels
                            , senvIsAuth = newIsAuth
                            , senvAuthUsers = newAuthUsers }

  newReplicaOffset <- newTVarIO 0
  let replicaEnv = ReplicaEnv sharedEnv newReplicaOffset

  case sharedCfg of
      SharedConfig _ _ _ (Slave _ _) -> runReplicaApp replicaEnv runReplica
      _ -> pure $ Right ("", "")

  putStrLn $ "Redis server listening on port " ++ port
  serve HostAny port $ \(socket, address) -> do
    putStrLn $ "successfully connected client: " ++ show address

    clientID <- atomically $ do
      i <- readTVar nextID
      writeTVar nextID (i + 1)
      pure i

    let newUserData = UserData "default" ["nopass"] []

    let cs = ClientState { multi = False
                         , multiList = []
                         , subscribeMode = False
                         , subscribeChannels = S.empty
                         , userData = newUserData
                         , isAuth = False }

    let clientCfg = ClientConfig clientID socket sharedCfg

    let env = ClientEnv sharedEnv clientCfg

    runClientApp env cs handleConnection
    closeSock socket

  where readDBFile store h = do
          magicWord <- BS.hGet h 5
          redisVersion <- BS.hGet h 4
          -- print $ "Header section: " <> magicWord <> " " <> redisVersion
          consumeMetadata h
          consumeDB h (msData store)
