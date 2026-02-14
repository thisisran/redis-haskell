{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

-- import Control.Exception (IOException)

import CliParser
import Control.Concurrent (threadDelay)
import qualified Control.Concurrent.Async as SA
import Control.Concurrent.STM
import qualified Control.Exception as CE
import Control.Monad (unless, void, when)
import Control.Monad.Except (throwError)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (asks)
import Control.Monad.State.Strict (gets, modify')
import Control.Monad.Trans.Except (ExceptT (..), runExceptT)
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT)
import qualified Crypto.Hash.SHA256 as SHA256
import qualified Data.ByteString as BS
import qualified Data.ByteString.Base16 as B16
import qualified Data.ByteString.Char8 as BS8
import qualified Data.HashMap.Strict as HM
import qualified Data.IntSet as IS
import Data.List (delete, foldl')
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe, isJust, isNothing)
import qualified Data.Set as S
import Data.Word (Word64)
import Encode
import Store
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, connect, recv, send, serve)
import RDBParser
import RedisParser
import System.Environment (getArgs)
import System.FilePath ((</>))
import System.IO (BufferMode (NoBuffering), Handle, IOMode (ReadMode), SeekMode (RelativeSeek), hPutStrLn, hSeek, hSetBuffering, stderr, stdout, withBinaryFile)
import Types
import UnliftIO (MonadUnliftIO, withRunInIO)
import qualified UnliftIO as UL
import qualified Utilities as U


updateReplicas :: (MonadStore m) => [BS.ByteString] -> m ()
updateReplicas command = do
  getReplication >>= \case
    Master {} -> do
       replSockets <- getReplicas
       socketList <- liftIO $ readTVarIO replSockets
       let encodedCommand = encodeArray True command
       currOffset <- getReplicaSentOffset
       setReplicaSentOffset (BS8.length encodedCommand + currOffset)
       go socketList encodedCommand
    _ -> pure ()
  where go [] cmd = pure ()
        go (x : xs) cmd = send x cmd >> go xs cmd

setCommand :: (MonadStore m) => BS.ByteString -> BS.ByteString -> Maybe SetExpiry -> m (Either BS.ByteString Response)
setCommand key val ex = do
  now <- liftIO U.nowNs
  setDataEntry key $ handleExpiry ex now

  let exCommand = case ex of
        Nothing -> []
        Just (EX n) -> ["ex", (BS8.pack . show) n]
        Just (PX n) -> ["px", (BS8.pack . show) n]

  pure $ Right $ RspContinue { resp = encodeSimpleString "OK", afterOp = updateReplicas $ ["SET", key, val] ++ exCommand, subsCred = NotSubsCmd "SET" }
  where
    handleExpiry Nothing timeRef = StoreEntry (StoreString val) Nothing
    handleExpiry (Just (EX ex)) timeRef = StoreEntry (StoreString val) $ Just (ExDurationMs (fromIntegral $ ex * 1_000), ExRef timeRef)
    handleExpiry (Just (PX px)) timeRef = StoreEntry (StoreString val) $ Just (ExDurationMs (fromIntegral px), ExRef timeRef)

getCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
getCommand key = do
  tv <- getDataEntry key
  case tv of
    Nothing -> pure $ Right $ RspNormal encodeNullBulkString (NotSubsCmd "GET")
    Just (StoreEntry (StoreString v) Nothing) -> pure $ Right $ RspNormal (encodeBulkString v) (NotSubsCmd "GET")
    Just (StoreEntry (StoreString v) (Just (ExDurationMs exDur, ExRef exRef))) -> do
      hasPassed <- liftIO $ U.hasElapsedSince exDur exRef
      if hasPassed
        then do
          delDataEntry key
          pure $ Right $ RspNormal encodeNullBulkString (NotSubsCmd "GET")
        else pure $ Right $ RspNormal (encodeBulkString v) (NotSubsCmd "GET")

pushCommand :: (MonadStore m) => BS.ByteString -> [BS.ByteString] -> PushCommand -> m (Either BS.ByteString Response)
pushCommand key values pushType = do
  tv <- getData
  let valuesCount = length values
  let pushArgs = if pushType == RightPushCmd then "RPUSH" else "LPUSH"
  pushResult <- liftIO . atomically $ do
    curr <- readTVar tv
    case M.lookup key curr of
      Nothing -> do
        writeTVar tv (M.insert key (StoreEntry (StoreList $ newItems pushType) Nothing) curr)
        pure $ Right valuesCount
      Just (StoreEntry (StoreList vs) Nothing) -> do
        let newLen = length vs + valuesCount
        writeTVar tv (M.insert key (StoreEntry (StoreList $ newList pushType vs) Nothing) curr)
        pure $ Right newLen
      _ -> pure $ Left ()

  case pushResult of
    Left () ->
      pure $ Right $ RspNormal "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" (NotSubsCmd pushArgs)
    Right newLen ->
      pure $ Right $ RspContinue
        { resp = encodeInteger newLen
        , afterOp = updateReplicas $ [pushArgs, key] ++ values
        , subsCred = NotSubsCmd pushArgs
        }
  where newItems RightPushCmd = values
        newItems LeftPushCmd = reverse values
        newList RightPushCmd oldList = oldList ++ values
        newList LeftPushCmd oldList = reverse values ++ oldList

lrangeCommand :: BS.ByteString -> Int -> Int -> ClientApp (Either BS.ByteString Response)
lrangeCommand key start stop = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ Right $ RspNormal (encodeArray True []) (NotSubsCmd "LRANGE")
    Just (StoreEntry (StoreList vs) Nothing) -> do
      pure $ Right $ RspNormal (encodeArray True $ go vs (normStart start) (normStop stop)) (NotSubsCmd "LRANGE")
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
    Nothing -> pure $ Right $ RspNormal (encodeInteger 0) (NotSubsCmd "LLEN")
    Just (StoreEntry (StoreList v) Nothing) -> pure $ Right $ RspNormal (encodeInteger $ length v) (NotSubsCmd "LLEN")

applyLPopHelper :: (MonadStore m) => BS.ByteString -> Int -> m (Either BS.ByteString Response)
applyLPopHelper key count = do
  tv <- getData
  popResult <- liftIO . atomically $ do
    curr <- readTVar tv
    case M.lookup key curr of
      Nothing -> pure $ Right Nothing
      Just (StoreEntry (StoreList v) Nothing)
        | null v -> pure $ Right Nothing
        | otherwise -> do
            let normCount = min count (length v)
            writeTVar tv (M.insert key (StoreEntry (StoreList (drop normCount v)) Nothing) curr)
            pure $ Right (Just (normCount, v))
      _ -> pure $ Left ()

  case popResult of
    Left () ->
      pure $ Right $ RspNormal "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n" (NotSubsCmd "LPOP")
    Right Nothing ->
      pure $ Right $ RspContinue
        { resp = encodeNullBulkString
        , afterOp = updateReplicas ["LPOP", key, (BS8.pack . show) count]
        , subsCred = NotSubsCmd "LPOP"
        }
    Right (Just (normCount, v)) ->
      pure $ Right $ RspContinue
        { resp = getPopped normCount v
        , afterOp = updateReplicas ["LPOP", key, (BS8.pack . show) count]
        , subsCred = NotSubsCmd "LPOP"
        }
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
            Left err -> pure $ Left err

blpopCommand :: BS.ByteString -> Double -> Int -> ClientApp (Either BS.ByteString Response)
blpopCommand key timeout clientID = do
  liftIO $ hPutStrLn stderr ("Starting BLPop with timeout: " <> show timeout)
  execWithTimeout timeout noTimeOp mainOp
  where
    noTimeOp = pure $ Right $ RspNormal encodeNullArray (NotSubsCmd "BLPOP")
    mainOp :: ClientApp (Either BS.ByteString (Bool, Response))
    mainOp = do
      val <- getDataEntry key
      case val of
        Nothing -> do
          addWaiterOnce key clientID
          pure $ Right (True, RspNormal mempty (NotSubsCmd "BLPOP"))
        Just (StoreEntry (StoreList []) Nothing) -> do
          addWaiterOnce key clientID
          pure $ Right (True, RspNormal mempty (NotSubsCmd "BLPOP"))
        Just (StoreEntry (StoreList (x : xs)) Nothing) -> do
          waiters <- getWaiterEntry key
          case waiters of
            Nothing -> do
              applyLPopHelper key 1 >>= \case
                Right (RspContinue resp _ _) -> pure $ Right (False, RspNormal (encodeArray False [encodeBulkString key, resp]) (NotSubsCmd "BLPOP"))
                Left _ -> pure $ Left "BLPop: An error has occurred"
            Just waitersList -> do
              if IS.member clientID waitersList
                then do
                  applyLPopHelper key 1 >>= \case
                    Right (RspContinue resp _ _) -> delWaiterEntry key >> pure (Right (False, RspNormal (encodeArray False [encodeBulkString key, resp]) (NotSubsCmd "BLPOP")))
                else do
                  addWaiterOnce key clientID
                  pure $ Right (True, RspNormal mempty $ NotSubsCmd "BLPOP")

typeCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
typeCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      (Streams streams) <- getStreams
      case HM.lookup key streams of
        Just _ -> pure $ Right $ RspNormal (encodeSimpleString "stream") (NotSubsCmd "TYPE")
        Nothing -> pure $ Right $ RspNormal (encodeSimpleString "none") (NotSubsCmd "TYPE")
    Just (StoreEntry (StoreString _) _) -> pure $ Right $ RspNormal (encodeSimpleString "string") (NotSubsCmd "TYPE")
    Just (StoreEntry (StoreList _) _) -> pure $ Right $ RspNormal (encodeSimpleString "list") (NotSubsCmd "TYPE")

xaddCommand :: (MonadStore m) => BS.ByteString -> EntryId -> RedisStreamValues -> m (Either BS.ByteString Response)
xaddCommand streamID (EntryId 0 0) values = pure $ Right $ RspNormal (encodeSimpleError RErrXAddGtThan0 mempty) (NotSubsCmd "XADD")
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
    Nothing -> addNewEntry M.empty streams replicaRep
    Just (x, _) ->
      if x >= entryID
        then pure $ Right $ RspNormal (encodeSimpleError RErrXaddEqSmallTargetItem mempty) (NotSubsCmd "XADD")
        else addNewEntry oldStream streams replicaRep
  where
    addNewEntry :: (MonadStore m) => M.Map EntryId RedisStreamValues -> RedisStreams -> [BS.ByteString] -> m (Either BS.ByteString Response)
    addNewEntry oldStream (Streams streams) replicaRep = do
      let newEntry = values
      let newStream = Stream (M.insert entryID newEntry oldStream)
      let newStreams = HM.insert streamID newStream streams
      setStreams (StoreEntry (StoreStreams (Streams newStreams)) Nothing)
      pure $ Right $ RspContinue { resp = encodeBulkString (U.entryIdToBS entryID), afterOp = updateReplicas replicaRep, subsCred = NotSubsCmd "XADD" }
    valuesToArray [] acc = acc
    valuesToArray ((key, value) : xs) acc = valuesToArray xs (acc ++ [key, value])

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
  pure $ Right $ RspNormal resp (NotSubsCmd "XRANGE")
  where
    parseKeysValues :: [(EntryId, RedisStreamValues)] -> BS.ByteString
    parseKeysValues keysValues = "*" <> (BS8.pack . show . length) keysValues <> "\r\n"
        <> foldr (\(id, valuesMap) acc -> "*2\r\n" <> encodeBulkString (U.entryIdToBS id) <> convertMap valuesMap <> acc) BS.empty keysValues
    convertMap :: RedisStreamValues -> BS.ByteString
    convertMap l = "*" <> (BS8.pack . show . (* 2) . length) l <> "\r\n"
        <> foldr (\(k, v) acc -> encodeBulkString k <> encodeBulkString v <> acc) BS.empty l

xrangeCommand :: BS.ByteString -> RangeEntryId -> RangeEntryId -> ClientApp (Either BS.ByteString Response)
xrangeCommand key mili RangeMinusPlus = do
  res <- xrangeEndHelper key (\_ _ -> True)
  case res of
    Nothing -> pure $ Right $ RspNormal encodeNullArray (NotSubsCmd "XRANGE")
    Just seq -> xrangeCommand key mili seq
xrangeCommand key RangeMinusPlus seq = do
  (filteredStream, Stream oldStream, streams) <- getStream key (M.lookupMin . M.filterWithKey (\_ _ -> True))
  case filteredStream of
    Nothing -> pure $ Right $ RspNormal encodeNullArray (NotSubsCmd "XRANGE")
    Just (EntryId m v, _) -> xrangeCommand key (RangeEntryId m v) seq
xrangeCommand key (RangeMili mili) seq@(RangeEntryId seq1 seq2) = xrangeCommand key (RangeEntryId mili 0) seq
xrangeCommand key mili@(RangeEntryId _ _) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ Right $ RspNormal (encodeSimpleError RErrXRangeIDNonExisting mempty) (NotSubsCmd "XRANGE")
    Just (RangeEntryId _ newEnd) -> xrangeCommand key mili (RangeEntryId seq newEnd)
xrangeCommand key (RangeMili mili) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ Right $ RspNormal (encodeSimpleError RErrXRangeIDNonExisting mempty) (NotSubsCmd "XRANGE")
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
            then pure $ Right $ RspNormal encodeNullArray (NotSubsCmd "XREAD")
            else do
              liftIO $ threadDelay 1_000
              go (elapsed + 0.001) newIds

xreadCommand keysIds Nothing = do
  result <- go keysIds BS.empty 0
  pure $ Right $ RspNormal ("*" <> (BS8.pack . show . length) keysIds <> "\r\n" <> result) (NotSubsCmd "XREAD")
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
            Right (RspNormal streamResp _) -> go xs (acc <> "*2\r\n" <> encodeBulkString stream_id <> streamResp) elapsed
            Left _ -> go xs acc elapsed

incrCommand :: (MonadStore m) => BS.ByteString -> m (Either BS.ByteString Response)
incrCommand key = do
  tv <- getData
  incrResult <- liftIO . atomically $ do
    curr <- readTVar tv
    case M.lookup key curr of
      Nothing -> do
        let nextVal = 1
        writeTVar tv (M.insert key (StoreEntry (StoreString "1") Nothing) curr)
        pure $ Right nextVal
      Just (StoreEntry (StoreString v) Nothing) -> case U.bsToInt v of
        Nothing -> pure $ Left ()
        Just i -> do
          let nextVal = i + 1
          writeTVar tv (M.insert key (StoreEntry (StoreString $ BS8.pack $ show nextVal) Nothing) curr)
          pure $ Right nextVal
      _ -> pure $ Left ()

  case incrResult of
    Left () -> pure $ Right $ RspNormal (encodeSimpleError RErrIncrNotIntegerOrRange mempty) (NotSubsCmd "INCR")
    Right nextVal -> pure $ Right $ RspContinue
      { resp = encodeInteger nextVal
      , afterOp = updateReplicas ["INCR", key]
      , subsCred = NotSubsCmd "INCR"
      }

execCommand :: ClientApp (Either BS.ByteString Response)
execCommand = do
  multi <- getMulti
  updateMulti False
  if multi
    then do
      ml <- getMultiList
      if null ml
        then pure $ Right $ RspNormal (encodeArray True []) (NotSubsCmd "EXEC")
        else do
          ml <- getMultiList
          res <- go ml []
          pure $ Right $ RspNormal (encodeArray False res) (NotSubsCmd "EXEC")
    else pure $ Right $ RspNormal (encodeSimpleError RErrExecNoMulti mempty) (NotSubsCmd "EXEC")
  where go [] acc = pure acc
        go (x : xs) acc = do
           eitherResp <- x
           case eitherResp of
                Right (RspNormal resp _) -> go xs (acc ++ [resp])
                Right (RspContinue resp _ _) -> go xs (acc ++ [resp])
                Left e -> go xs acc

discardCommand :: ClientApp (Either BS.ByteString Response)
discardCommand = do
  multi <- getMulti
  if multi
    then do
      updateMulti False
      resetMultiCommands
      pure $ Right $ RspNormal (encodeSimpleString "OK") (NotSubsCmd "DISCARD")
    else pure $ Right $ RspNormal (encodeSimpleError RErrDiscardNoMulti mempty) (NotSubsCmd "DISCARD")

handleMultiCmd :: ClientApp (Either BS.ByteString Response) -> ClientApp (Either BS.ByteString Response)
handleMultiCmd op = do
  multi <- getMulti
  if multi
    then do
      addMultiCommand op
      pure $ Right $ RspNormal (encodeSimpleString "QUEUED") (NotSubsCmd mempty)
    else op

infoCommand :: InfoRequest -> ClientApp (Either BS.ByteString Response)
infoCommand Replication = do
  role <- getRole
  case role of
    Master repID repOffset -> pure $ Right $ RspNormal (masterResponse repID repOffset) (NotSubsCmd "INFO")
    Slave roHost roPort -> pure $ Right $ RspNormal (encodeBulkString "# Replication\nrole:slave") (NotSubsCmd "INFO")
  where
    masterResponse rID repOS = encodeBulkString $ "# Replication\nrole:master\nmaster_replid:" <> BS8.pack rID <> "\nmaster_repl_offset:" <> (BS8.pack . show) repOS

replConfCommand :: ReplConfOptions -> ClientApp (Either BS.ByteString Response)
replConfCommand (ListeningPort port) = do
  pure $ Right $ RspNormal (encodeSimpleString "OK") (NotSubsCmd "REPLCONF listening port")
replConfCommand (Capa capa) = do
  pure $ Right $ RspNormal (encodeSimpleString "OK") (NotSubsCmd "REPLCONF Capa")
replConfCommand (AckWith offset) = do
  serverOffset <- getReplicaSentOffset
  if offset == serverOffset
    then do
      tvComplete <- asks $ senvCompleteReplicaCount . cenvShared
      liftIO . atomically $ do
        current <- readTVar tvComplete
        writeTVar tvComplete $ current + 1
      current <- liftIO $ readTVarIO tvComplete
      pure $ Right $ RspNormal mempty (NotSubsCmd "REPLCONF AckWith")
    else pure $ Right $ RspNormal mempty (NotSubsCmd "REPLCONF AckWith")

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
      pure $ Right $ RspContinue { resp = encodeSimpleString $ "FULLRESYNC " <> BS8.pack repID <> " 0", afterOp = sendSnapshot, subsCred = NotSubsCmd "PSYNC" }
    _ -> pure $ Right $ RspNormal mempty (NotSubsCmd "PSYNC") -- A slave will never get a PSync command from a client

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

  sendAcknowledgements serverOffset tvComplete replicas
  currOffset <- getReplicaSentOffset
  if currOffset > 0
    then do
      liftIO $ hPutStrLn stderr "Sent ACK to all clients"
      execWithTimeout (timeout / 1_000) countSoFarOp countFullOp
    else pure $ Right $ RspNormal (encodeInteger $ length replicas) (NotSubsCmd "WAIT")
  where
    sendAcknowledgements serverOS tvComplete [] = pure ()
    sendAcknowledgements serverOS tvComplete (x : xs) = do
      liftIO $ sendAckCommand x serverOS tvComplete
      sendAcknowledgements serverOS tvComplete xs
    countSoFarOp :: ClientApp (Either BS.ByteString Response)
    countSoFarOp = do
      result <- getCompleteReplicas
      updateServerSent
      pure $ Right $ RspNormal (encodeInteger result) (NotSubsCmd "WAIT")
    countFullOp :: ClientApp (Either BS.ByteString (Bool, Response))
    countFullOp = do
      result <- getCompleteReplicas
      if result >= reqReady
        then do
          updateServerSent
          pure $ Right (False, RspNormal (encodeInteger result) (NotSubsCmd "WAIT"))
        else pure $ Right (True, RspNormal mempty (NotSubsCmd "WAIT"))
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
  pure $ Right $ RspNormal (encodeArray True ["dir", BS8.pack dir]) (NotSubsCmd "CONFIG")
configCommand ConfigGetFileName = do
  fileName <- asks $ cfgRDBFileName . ccfgShared . cenvConfig
  pure $ Right $ RspNormal (encodeArray True ["dbfilename", BS8.pack fileName]) (NotSubsCmd "CONFIG")

-- TODO: Filtering is VERY inefficient, redo at some point
keysCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
keysCommand "*" = do
  tvStore <- getData
  store <- liftIO $ readTVarIO tvStore
  let result = M.keys store
  pure $ Right $ RspNormal (encodeArray True result) (NotSubsCmd "KEYS")
keysCommand filter = do
  tvStore <- getData
  store <- liftIO $ readTVarIO tvStore
  let result = M.keys store
  pure $ Right $ RspNormal (encodeArray True (foldl' foldme [] result)) (NotSubsCmd "KEYS")
  where
    foldme :: [BS.ByteString] -> BS.ByteString -> [BS.ByteString]
    foldme acc item =
      if go (BS8.unpack filter) (BS8.unpack item)
        then acc ++ [BS8.pack (BS8.unpack item)]
        else acc
    go :: String -> String -> Bool
    go [] [] = True
    go [] _ = False
    go ('*' : _) [] = True
    go (x : _) [] = False
    go (x : xs) (y : ys)
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
  pure $ Right $ RspNormal (encodeArray False [encodeBulkString "subscribe", encodeBulkString channel, encodeInteger $ length currChannels]) SubscribeCmd

publishCommand :: BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
publishCommand channel msg = do
  socket <- getSocket
  clientList <- getChannelClients channel
  sendMessage clientList
  pure $ Right $ RspNormal (encodeInteger $ length clientList) SubscribeCmd
  where
    sendMessage [] = pure ()
    sendMessage (x : xs) = do
      send x $ encodeArray True ["message", channel, msg]
      sendMessage xs

zaddCommand :: BS.ByteString -> Double -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
zaddCommand name score member = do
  oldCount <- getZSetMemberCount name member
  addMemberToZSet name score member
  currCount <- getZSetMemberCount name member
  pure $ Right $ RspNormal (encodeInteger $ currCount - oldCount) (NotSubsCmd "ZADD")

zrankCommand :: BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
zrankCommand name member = do
  (ZSet scoreMap memberDict) <- getZSet name
  maybeVal <- runMaybeT $ do
    score <- MaybeT . pure $ HM.lookup member memberDict
    let precedingSets = M.elems (fst (M.split score scoreMap))
    let precedingCount = (sum . map S.size) precedingSets
    memberSet <- MaybeT . pure $ M.lookup score scoreMap
    rank <- MaybeT . pure $ S.lookupIndex member memberSet
    pure $ RspNormal (encodeInteger (precedingCount + rank)) (NotSubsCmd "ZRANK")
  case maybeVal of
    Just result -> pure $ Right result
    Nothing -> pure $ Right $ RspNormal encodeNullBulkString (NotSubsCmd "ZRANK")

zrangeCommand :: BS.ByteString -> Int -> Int -> ClientApp (Either BS.ByteString Response)
zrangeCommand name start end = do
  (ZSet scoreMap _) <- getZSet name
  let allScores = M.elems scoreMap
  let allScoresList = concatMap S.toAscList allScores
  let itemCount = length allScoresList
  let nStart = normStart start itemCount
  let nEnd = normStop end itemCount
  pure $ Right $ RspNormal (encodeArray True (take (nEnd - nStart + 1) $ drop nStart allScoresList)) (NotSubsCmd "ZRANGE")
  where
    normStart index itemCount
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
  pure $ Right $ RspNormal (encodeInteger $ (sum . map S.size) $ M.elems scoreMap) (NotSubsCmd "ZCARD")

zscoreCommand :: BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
zscoreCommand name member = do
  (ZSet _ memberDict) <- getZSet name
  case HM.lookup member memberDict of
    Just score -> pure $ Right $ RspNormal (encodeBulkString (BS8.pack $ show score)) (NotSubsCmd "ZSCORE")
    Nothing -> pure $ Right (RspNormal encodeNullBulkString (NotSubsCmd "ZSCORE"))

zremCommand :: BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
zremCommand name member = do
  (ZSet scoreMap memberDict) <- getZSet name
  case HM.lookup member memberDict of
    Just score ->
      let updatedMemberDict = HM.delete member memberDict
       in case M.lookup score scoreMap of
            Just memberSet -> do
              let newMap = M.alter (Just . S.delete member . fromMaybe S.empty) score scoreMap
              tv <- getZSets
              liftIO . atomically $
                modifyTVar' tv $
                  HM.alter (Just . const (ZSet newMap updatedMemberDict) . fromMaybe (ZSet M.empty HM.empty)) name
              pure $ Right $ RspNormal (encodeInteger 1) (NotSubsCmd "ZREM")
            Nothing -> pure $ Right $ RspNormal (encodeInteger 0) (NotSubsCmd "ZREM")
    Nothing -> pure $ Right $ RspNormal (encodeInteger 0) (NotSubsCmd "ZREM")

geoAddCommand :: BS.ByteString -> Double -> Double -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
geoAddCommand name longitude latitude member = do
  if longitude >= 180 || longitude <= -180
    then pure $ Right $ RspNormal (encodeSimpleError RErrGeoAddLongRange mempty) (NotSubsCmd "GEOADD")
    else
      if latitude >= 85.05112878 || latitude <= -85.05112878
        then pure $ Right $ RspNormal (encodeSimpleError RErrGeoAddLatRange mempty) (NotSubsCmd "GEOADD")
        else do
          zaddCommand name (U.interleaveGeo latitude longitude) member >>= \case
            Right (RspNormal resp _) -> pure $ Right $ RspNormal (encodeInteger 1) (NotSubsCmd "GEOADD")
            Left _ -> pure $ Left "Error in GeoAdd Command"

geoPosCommand :: BS.ByteString -> [BS.ByteString] -> ClientApp (Either BS.ByteString Response)
geoPosCommand name members = do
  (ZSet _ memberDict) <- getZSet name
  let values = map (`HM.lookup` memberDict) members
  pure $ Right $ RspNormal (encodeArray False (go values [])) (NotSubsCmd "GEOPOS")
  where
    go :: [Maybe Double] -> [BS.ByteString] -> [BS.ByteString]
    go [] acc = acc
    go (x : xs) acc = do
      case x of
        Just score ->
          let (lat, long) = U.deinterleaveGeo score
           in go xs $ acc ++ [encodeArray True [(BS8.pack . show) long, (BS8.pack . show) lat]]
        Nothing -> go xs $ acc ++ [encodeNullArray]

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
    Just dist -> pure $ Right $ RspNormal (encodeBulkString ((BS8.pack . show) dist)) (NotSubsCmd "GEODIST")
    Nothing -> pure $ Right $ RspNormal (encodeSimpleError RErrGeoDistMissingMember mempty) (NotSubsCmd "GEODIST")

geoSearchCommand :: BS.ByteString -> Double -> Double -> Double -> DistUnit -> ClientApp (Either BS.ByteString Response)
geoSearchCommand name longitude latitude radius unit = do
  (ZSet _ memberDict) <- getZSet name
  let normRadius = case unit of
        DistKilometer -> radius * 1_000
        DistMile -> radius * 1609.344
        _ -> radius
  let membersInRange = HM.foldrWithKey' (\member score acc -> if calcDistance score longitude latitude <= normRadius then acc ++ [member] else acc) [] memberDict
  pure $ Right $ RspNormal (encodeArray True membersInRange) (NotSubsCmd "GEOSEARCH")
  where
    calcDistance score centerLong centerLat =
      let (lat1, long1) = U.deinterleaveGeo score
       in U.calcGeoDistance (long1, lat1) (centerLong, centerLat)

aclCommand :: AclSubCmd -> ClientApp (Either BS.ByteString Response)
aclCommand opt =
  case opt of
    AclWhoAmI -> do
      UserData {name = n} <- gets userData
      pure $ Right $ RspNormal (encodeBulkString n) (NotSubsCmd "ACL")
    AclGetUser user -> do
      UserData {flags = fl, passwords = ps} <- gets userData
      let response = [encodeBulkString "flags", encodeArray True fl, encodeBulkString "passwords", encodeArray True ps]
      pure $ Right $ RspNormal (encodeArray False response) (NotSubsCmd "ACL")
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

      modify' (\cs -> cs {userData = UserData {name = user, passwords = newPasswords, flags = newFlags}})
      pure $ Right $ RspNormal (encodeSimpleString "OK") (NotSubsCmd "ACL")

authCommand :: BS.ByteString -> BS.ByteString -> ClientApp (Either BS.ByteString Response)
authCommand userName password = do
  let encodedPass = (B16.encode . SHA256.hash) password
  UserData {name = user, flags = fl, passwords = ps} <- gets userData
  tvUserPass <- asks $ senvAuthUsers . cenvShared
  userPassMap <- liftIO $ readTVarIO tvUserPass
  tvServerAuth <- asks $ senvIsAuth . cenvShared
  isServerAuth <- liftIO $ readTVarIO tvServerAuth
  if isServerAuth
    then case HM.lookup userName userPassMap of
      Just serverPass ->
        if serverPass == encodedPass
          then do
            let newPasswords = ps ++ [encodedPass]
            modify' (\cs -> cs {isAuth = True, userData = UserData {name = user, flags = fl, passwords = newPasswords}})
            pure $ Right $ RspNormal (encodeSimpleString "OK") (NotSubsCmd "AUTH")
          else pure $ Right $ RspNormal (encodeSimpleError RErrAuthInvalidUserName mempty) (NotSubsCmd "AUTH")
      Nothing -> pure $ Right $ RspNormal (encodeSimpleError RErrAuthServerAuthUserNotFound mempty) (NotSubsCmd "AUTH")
    else do
      if user == userName && elem encodedPass ps
        then do
          modify' (\cs -> cs {isAuth = True}) -- this client is now authcenticated
          pure $ Right $ RspNormal (encodeSimpleString "OK") (NotSubsCmd "AUTH")
        else do
          pure $ Right $ RspNormal (encodeSimpleError RErrAuthInvalidUserName mempty) (NotSubsCmd "AUTH")

isAuthorizedCmd :: Command -> ClientApp Bool
isAuthorizedCmd cmd = do
  tvServerAuth <- asks $ senvIsAuth . cenvShared
  isServerAuth <- liftIO $ readTVarIO tvServerAuth
  isUserAuth <- gets isAuth
  isAuthCommand <- case cmd of Auth {} -> pure True; _ -> pure False
  pure $ (isServerAuth && (isUserAuth || isAuthCommand)) || not isServerAuth

handleSubsMode :: Either BS.ByteString Response -> ClientApp ()
handleSubsMode resp = do
  sock <- getSocket
  subMode <- getSubscribed
  if subMode then
    case resp of
      Right (RspNormal resp (NotSubsCmd cmdName)) -> liftIO $ send sock $ encodeSimpleError RErrSubUnauthorizedCmd cmdName
      Right (RspNormal resp SubscribeCmd) -> liftIO $ send sock resp
      Right (RspNormal resp PingCmd) -> liftIO $ send sock $ encodeArray True ["pong", ""]
      Right (RspContinue resp nextAction (NotSubsCmd cmdName)) -> liftIO $ send sock $ encodeSimpleError RErrSubUnauthorizedCmd cmdName
      Right (RspContinue resp nextAction SubscribeCmd) -> liftIO (send sock resp) >> nextAction
      Left _ -> pure ()
  else case resp of
      Right (RspNormal resp _) -> liftIO $ send sock resp
      Right (RspContinue resp nextAction _) -> liftIO (send sock resp) >> nextAction
      Left _ -> pure ()

unsubcribeCommand :: BS.ByteString -> ClientApp (Either BS.ByteString Response)
unsubcribeCommand channel = do
  socket <- getSocket
  setSubscribed False
  removeSubChannel channel
  removeChannelSubscriber channel socket
  remaining <- getSubChannels
  pure $ Right $ RspNormal (encodeArray False [encodeBulkString "unsubscribe", encodeBulkString channel, encodeInteger (S.size remaining)]) SubscribeCmd

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
           isAuth <- isAuthorizedCmd cmd
           if isAuth then do
             cmdResp <- case cmd of
               Ping -> handleMultiCmd $ pure $ Right $ RspNormal (encodeSimpleString "PONG") PingCmd
               (Echo str) -> handleMultiCmd $ pure $ Right $ RspNormal (encodeBulkString str) (NotSubsCmd "ECHO")
               (Set key val args) -> handleMultiCmd $ setCommand key val args
               (Get key) -> handleMultiCmd $ getCommand key
               (RPush key values) -> handleMultiCmd $ pushCommand key values RightPushCmd
               (LPush key values) -> handleMultiCmd $ pushCommand key values LeftPushCmd
               (LRange key start stop) -> handleMultiCmd $ lrangeCommand key start stop
               (LLen key) -> handleMultiCmd $ llenCommand key
               (LPop key count) -> handleMultiCmd $ lpopCommand key count
               (BLPop key timeout) -> getClientID >>= \clientID -> handleMultiCmd $ blpopCommand key timeout clientID
               (Type key) -> handleMultiCmd $ typeCommand key
               (XAdd streamID entryID values) -> handleMultiCmd $ xaddCommand streamID entryID values
               (XRange key start end) -> handleMultiCmd $ xrangeCommand key start end
               (XRead keysIds timeout) -> handleMultiCmd $ xreadCommand keysIds timeout
               (Incr key) -> handleMultiCmd $ incrCommand key
               Multi -> updateMulti True >> pure (Right $ RspNormal (encodeSimpleString "OK") (NotSubsCmd "MULTI"))
               Exec -> execCommand
               Discard -> discardCommand
               (Info infoRequest) -> handleMultiCmd $ infoCommand infoRequest
               (ReplConf replOptions) -> replConfCommand replOptions
               (Psync req) -> psyncCommand req
               (Wait replicaNum timeout) -> waitCommand replicaNum timeout
               (Config opt) -> configCommand opt
               (Keys filter) -> keysCommand filter
               (Subscribe channel) -> subscribeCommand channel
               (Unsubscribe channel) -> unsubcribeCommand channel
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
               Cmd -> pure $ Right $ RspNormal encodeEmptyArray SubscribeCmd -- convenience command for running redis-cli in bulk mode
             handleSubsMode cmdResp
           else liftIO $ send sock (encodeSimpleError RErrAuthRequired mempty)
           go rest -- rest may already contain next command
         RParserErr e -> liftIO $ send sock e

---------------------------------------------------------------------------------------------------------
-- Slave functions

recvByParser :: (BS.ByteString -> StringParserResult) -> Socket -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
recvByParser parser sock = go
  where
    go acc = do
      case parser acc of
        SParserFullString str rest -> pure $ Right (str, rest)
        SParserPartialString ->
          recv sock 4096 >>= \case
            Nothing -> pure $ Left "Received only partial response from the (Master) server"
            Just chunk -> go (acc <> chunk)
        SParserError msg -> pure $ Left msg

recvSimpleResponse :: Socket -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
recvSimpleResponse = recvByParser parseSimpleString

recvRDBFile :: Socket -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
recvRDBFile = recvByParser parseRDBFile

getAckCommand :: ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
getAckCommand = do
  offset <- getReplicaOffset
  pure $ Right (encodeArray True ["REPLCONF", "ACK", (BS8.pack . show) offset], "")

awaitServerUpdates :: Socket -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
awaitServerUpdates sock buf = go 0 buf
  where
    go :: Int -> BS.ByteString -> ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
    go offset acc = do
     setReplicaOffset offset
     case parseOneCommand acc of
       RParsed cmd rest -> do
         let processedCount = BS.length acc - BS.length rest
         applied <- case cmd of
           Ping ->
             pure $ Right ()
           Set key val args ->
             setCommand key val args >>= \case
               Right _ -> pure $ Right ()
               Left _ -> pure $ Left (BS8.pack "Err (Replica): Set command")
           RPush key values ->
             pushCommand key values RightPushCmd >>= \case
               Right _ -> pure $ Right ()
               Left _ -> pure $ Left "Err (Replica): RPush command"
           LPush key values ->
             pushCommand key values LeftPushCmd >>= \case
               Right _ -> pure $ Right ()
               Left _ -> pure $ Left "Err (Replica): LPush command"
           LPop key count ->
             applyLPopHelper key count >>= \case
               Right _ -> pure $ Right ()
               Left _ -> pure $ Left "Err (Replica): LPop command"
           XAdd streamID entryID v ->
             xaddCommand streamID entryID v >>= \case
               Right _ -> pure $ Right ()
               Left _ -> pure $ Left "Err (Replica): XAdd command"
           Incr key ->
             incrCommand key >>= \case
               Right _ -> pure $ Right ()
               Left _ -> pure $ Left "Err (Replica): Incr command"
           ReplConf GetAck -> do
             eitherResp <- getAckCommand
             case eitherResp of
               Right (resp, _) -> liftIO (send sock resp) >> pure (Right ())
               Left _ -> pure $ Left "Error (Replica): unknown command from the server"
           _ -> pure $ Left "Error (Replica): unknown command from the server"
         case applied of
           Left err -> pure $ Left err
           Right _ -> go (offset + processedCount) rest
       RParserNeedMore -> do
         mbMore <- liftIO $ recv sock 4096
         case mbMore of
           Nothing -> pure $ Left "Error (Replica): Incomplete command received and server seemed to have closed the connection"
           Just more -> go offset (acc <> more)
       RParserErr _ -> pure $ Left "Error (Replica): Command received from server coud not be parsed properly"

runReplica :: ReplicaApp (Either BS.ByteString (BS.ByteString, BS.ByteString))
runReplica = do
  clientPort <- getPort
  getReplication >>= \case
    Master _ _ -> pure $ Right (mempty, mempty)
    -- withRunInIO :: ((ReplicaApp () -> IO ()) -> IO) -> ReplicaApp()
    Slave host port -> withRunInIO $ \runInIO -> do
      putStrLn ("Trying to connect to " <> host <> " " <> port)
      _ <- UL.async $ connect host port $ \(_sock, _addr) ->
        -- runInIO :: ReplicaApp () -> IO ()
        runInIO $ do
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
            (resp, _pending5) <- ExceptT $ awaitServerUpdates _sock pending4
            pure resp

          case e of
            Left err -> liftIO $ putStrLn ("(Replica) failed with: " <> show err)
            Right resp -> liftIO $ print resp
      pure $ Right (mempty, mempty)

----------- Slave functions END

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering

  cfgCli <- parseCli

  let port = fromMaybe "6379" (cliPort cfgCli)
  let dir = fromMaybe "/tmp/redis-files" (cliDir cfgCli)
  let rdbFileName = fromMaybe "dump.rdb" (cliFileName cfgCli)

  store <- newMemoryStore

  CE.try (withBinaryFile (dir </> rdbFileName) ReadMode (readDBFile store)) :: IO (Either CE.IOException ())

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

  let sharedEnv =
        SharedEnv
          { senvStore = store,
            senvSets = newZSets,
            senvConfig = sharedCfg,
            senvReplicas = newReplicas,
            senvReplicaSentOffset = sentOffset,
            senvCompleteReplicaCount = complReplicas,
            senvChannels = newChannels,
            senvIsAuth = newIsAuth,
            senvAuthUsers = newAuthUsers
          }

  newReplicaOffset <- newTVarIO 0
  let replicaEnv = ReplicaEnv sharedEnv newReplicaOffset

  case sharedCfg of
    SharedConfig _ _ _ (Slave _ _) -> runReplicaApp replicaEnv runReplica
    _ -> pure $ Right (mempty, mempty)

  nextID <- newTVarIO (0 :: Int)

  putStrLn $ "Redis server listening on port " ++ port
  serve HostAny port $ \(socket, address) -> do
    putStrLn $ "successfully connected client: " ++ show address

    clientID <- atomically $ do
      i <- readTVar nextID
      writeTVar nextID (i + 1)
      pure i

    let newUserData = UserData "default" ["nopass"] []

    let cs =
          ClientState
            { multi = False,
              multiList = [],
              subscribeMode = False,
              subscribeChannels = S.empty,
              userData = newUserData,
              isAuth = False
            }

    let clientCfg = ClientConfig clientID socket sharedCfg

    let env = ClientEnv sharedEnv clientCfg

    runClientApp env cs handleClientConnection
    closeSock socket
  where
    readDBFile store h = do
      magicWord <- BS.hGet h 5
      redisVersion <- BS.hGet h 4
      consumeMetadata h
      consumeDB h (msData store)
