module Command
  ( runClientCommand
  , setCommand
  , pushCommand
  , applyLPopHelper
  , xaddCommand
  , incrCommand)
  where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Reader (asks)
import Control.Monad.State.Strict (gets, modify')
import Control.Monad.Trans.Maybe (runMaybeT, hoistMaybe)


import Control.Concurrent.STM
import Network.Simple.TCP (Socket, send)

import Data.Maybe (fromMaybe)
import Data.List (delete, foldl')
import Data.Foldable (for_)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import qualified Data.Set as S

import qualified Control.Concurrent.Async as SA

import qualified Data.ByteString.Base16 as B16
import qualified Crypto.Hash.SHA256 as SHA256

import Types
import Store
import Encode

import qualified Utilities as U

applyEffect :: Effect -> ClientApp ()
applyEffect (EffUpdateReplica cmd) = updateReplicas cmd
applyEffect EffSnapshot = do
  socket <- getSocket
  case U.decodeRdbBase64 U.emptyRdbFile of
    Right x -> liftIO $ send socket $ encodeRdbFile x
applyEffect (EffPublishChannel channel msg) = do
  clients <- getChannelClients channel
  for_ clients $ \s -> send s payload
  where payload = encodeArray True ["message", channel, msg]

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

setCommand :: (MonadStore m) => BS.ByteString -> BS.ByteString -> Maybe SetExpiry -> m Response
setCommand key val ex = do
  now <- liftIO U.nowMS
  setDataEntry key $ handleExpiry ex now

  let exCommand = case ex of
        Nothing -> []
        Just (EX n) -> ["ex", (BS8.pack . show) n]
        Just (PX n) -> ["px", (BS8.pack . show) n]

  pure $ RspContinue { resp = encodeSimpleString "OK", effect = EffUpdateReplica $ ["SET", key, val] ++ exCommand, cmdName = "SET" }
  where
    handleExpiry Nothing timeRef = StoreEntry (StoreString val) Nothing
    handleExpiry (Just (EX ex)) timeRef = StoreEntry (StoreString val) $ Just (ExDurationMs (fromIntegral $ ex * 1_000), ExRef timeRef)
    handleExpiry (Just (PX px)) timeRef = StoreEntry (StoreString val) $ Just (ExDurationMs (fromIntegral px), ExRef timeRef)

getCommand :: BS.ByteString -> ClientApp Response
getCommand key = do
  tv <- getDataEntry key
  case tv of
    Nothing -> pure $ RspNormal { resp = encodeNullBulkString, cmdName = "GET" }
    Just (StoreEntry (StoreString v) Nothing) -> pure $ RspNormal { resp = encodeBulkString v, cmdName = "GET" }
    Just (StoreEntry (StoreString v) (Just (ExDurationMs exDur, ExRef exRef))) -> do
      hasPassed <- liftIO $ U.hasElapsedSince exDur exRef
      if hasPassed
        then do
          delDataEntry key
          pure $ RspNormal { resp = encodeNullBulkString, cmdName = "GET" }
        else pure $ RspNormal { resp = encodeBulkString v, cmdName = "GET" }

pushCommand :: (MonadStore m) => BS.ByteString -> [BS.ByteString] -> PushCommand -> m Response
pushCommand key values pushType = do
  tvStoreData <- getData
  let valuesCount = length values
  let pushArgs = if pushType == RightPushCmd then "RPUSH" else "LPUSH"

  pushResult <- liftIO . atomically $ do
    storeData <- readTVar tvStoreData
    case M.lookup key storeData of
      Nothing -> do
        writeTVar tvStoreData (M.insert key (StoreEntry (StoreList $ newItems pushType) Nothing) storeData)
        pure (Just valuesCount)
      Just (StoreEntry (StoreList vs) Nothing) -> do
        let newLen = length vs + valuesCount
        writeTVar tvStoreData (M.insert key (StoreEntry (StoreList $ newList pushType vs) Nothing) storeData)
        pure $ Just newLen
      _ -> pure Nothing

  case pushResult of
    Nothing ->
      pure $ RspNormal { resp = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", cmdName = pushArgs }
    Just newLen ->
      pure $ RspContinue
        { resp = encodeInteger newLen
        , effect = EffUpdateReplica $ [pushArgs, key] ++ values
        , cmdName = pushArgs
        }
  where newItems RightPushCmd = values
        newItems LeftPushCmd = reverse values
        newList RightPushCmd oldList = oldList ++ values
        newList LeftPushCmd oldList = reverse values ++ oldList

lrangeCommand :: BS.ByteString -> Int -> Int -> ClientApp Response
lrangeCommand key start stop = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ RspNormal { resp = encodeArray True [], cmdName = "LRANGE" }
    Just (StoreEntry (StoreList vs) Nothing) -> do
      pure $ RspNormal { resp = encodeArray True $ go vs (normStart start) (normStop stop), cmdName = "LRANGE" }
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

llenCommand :: BS.ByteString -> ClientApp Response
llenCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ RspNormal { resp = encodeInteger 0, cmdName = "LLEN" }
    Just (StoreEntry (StoreList v) Nothing) -> pure $ RspNormal { resp = encodeInteger $ length v, cmdName = "LLEN" }

applyLPopHelper :: TVar StoreData -> BS.ByteString -> Int -> STM Response
applyLPopHelper tvData key count = do
    curr <- readTVar tvData
    case M.lookup key curr of
      Nothing -> pure $ RspNormal { resp = encodeNullBulkString, cmdName = "LPOP" }
      Just (StoreEntry (StoreList v) Nothing)
        | null v -> pure $ RspNormal { resp = encodeNullBulkString, cmdName = "LPOP" }
        | otherwise -> do
            let normCount = min count (length v)
            writeTVar tvData (M.insert key (StoreEntry (StoreList (drop normCount v)) Nothing) curr)
            pure $ RspContinue { resp = getPopped normCount v, effect = EffUpdateReplica ["LPOP", key, (BS8.pack . show) count], cmdName = "LPOP" }
      _ -> pure $ RspNormal { resp = encodeSimpleError RErrPopWrongValueType mempty, cmdName = "LPOP" }
  where
    getPopped 1 (x : _) = encodeBulkString x
    getPopped popCount xs = encodeArray True (take popCount xs)

lpopCommand :: BS.ByteString -> Int -> ClientApp Response
lpopCommand key count = do
  tv <- asks $ (.sData) . senvStore . cenvShared
  (liftIO . atomically) (applyLPopHelper tv key count) >>= \resp -> pure resp

typeCommand :: BS.ByteString -> ClientApp Response
typeCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      streams <- getStreams
      case HM.lookup key streams of
        Just _ -> pure $ RspNormal { resp = encodeSimpleString "stream", cmdName = "TYPE" }
        Nothing -> pure $ RspNormal { resp = encodeSimpleString "none", cmdName = "TYPE" }
    Just (StoreEntry (StoreString _) _) -> pure $ RspNormal { resp = encodeSimpleString "string", cmdName = "TYPE" }
    Just (StoreEntry (StoreList _) _) -> pure $ RspNormal { resp = encodeSimpleString "list", cmdName = "TYPE" }

xaddCommand :: (MonadStore m) => BS.ByteString -> EntryId -> [BSPair] -> m Response
xaddCommand streamID (EntryId 0 0) values = pure $ RspNormal { resp = encodeSimpleError RErrXAddGtThan0 mempty, cmdName = "XADD" }
xaddCommand streamID EntryGenNew values = liftIO U.nowMS >>= \now -> xaddCommand streamID (EntryGenSeq (fromIntegral now)) values
xaddCommand streamID (EntryGenSeq mili) values = do
  streams <- getStreams
  let (filteredStream, _, _) = getStream streams streamID (M.lookupMax . M.filterWithKey (\(EntryId m _) _ -> m == mili))
  case filteredStream of
    Nothing -> xaddCommand streamID (EntryId mili (if mili == 0 then 1 else 0)) values
    Just (EntryId m v, _) -> xaddCommand streamID (EntryId mili (v + 1)) values
xaddCommand streamID entryID@(EntryId mili seq) values = do
  streams <- getStreams
  let (filteredStream, oldStream, _) = getStream streams streamID (M.lookupMax . M.filterWithKey (\_ _ -> True))
  let replicaRep = ["XADD", streamID, U.entryIdToBS entryID] ++ valuesToArray values []
  case filteredStream of
    Nothing -> addNewEntry M.empty streams replicaRep
    Just (x, _) ->
      if x >= entryID
        then pure $ RspNormal { resp = encodeSimpleError RErrXaddEqSmallTargetItem mempty, cmdName = "XADD" }
        else addNewEntry oldStream streams replicaRep
  where
    addNewEntry :: (MonadStore m) => Stream -> Streams -> [BS.ByteString] -> m Response
    addNewEntry oldStream streams replicaRep = do
      let newEntry = values
      let newStream = M.insert entryID newEntry oldStream
      let newStreams = HM.insert streamID newStream streams
      setStreams newStreams
      pure $ RspContinue { resp = encodeBulkString (U.entryIdToBS entryID), effect = EffUpdateReplica replicaRep, cmdName = "XADD" }
    valuesToArray [] acc = acc
    valuesToArray ((key, value) : xs) acc = valuesToArray xs (acc ++ [key, value])

xrangeEndHelper :: BS.ByteString -> (EntryId -> [BSPair] -> Bool) -> ClientApp (Maybe RangeEntryId)
xrangeEndHelper key f = do
  streams <- getStreams
  let (filteredStream, _, _) = getStream streams key (M.lookupMax . M.filterWithKey f)
  case filteredStream of
    Nothing -> pure Nothing
    Just (EntryId m v, _) -> pure $ Just (RangeEntryId m v)

xrangeHelper :: Streams -> BS.ByteString -> (EntryId -> EntryId -> Bool) -> RangeEntryId -> RangeEntryId -> Response
xrangeHelper streams key rangef (RangeEntryId mili1 mili2) (RangeEntryId seq1 seq2) =
  let (_, oldStream, _) = getStream streams key (const Nothing . M.filterWithKey (\_ _ -> True))
      allKeysValues = M.toAscList (U.range rangef (EntryId mili1 mili2) (EntryId seq1 seq2) oldStream)
      resp = parseKeysValues allKeysValues
  in RspNormal { resp = resp, cmdName = "XRANGE" }
  where
    parseKeysValues :: [(EntryId, [BSPair])] -> BS.ByteString
    parseKeysValues keysValues = "*" <> (BS8.pack . show . length) keysValues <> "\r\n"
        <> foldr (\(id, valuesMap) acc -> "*2\r\n" <> encodeBulkString (U.entryIdToBS id) <> convertMap valuesMap <> acc) BS.empty keysValues
    convertMap :: [BSPair] -> BS.ByteString
    convertMap l = "*" <> (BS8.pack . show . (* 2) . length) l <> "\r\n"
        <> foldr (\(k, v) acc -> encodeBulkString k <> encodeBulkString v <> acc) BS.empty l

xrangeCommand :: BS.ByteString -> RangeEntryId -> RangeEntryId -> ClientApp Response
xrangeCommand key mili RangeMinusPlus = do
  res <- xrangeEndHelper key (\_ _ -> True)
  case res of
    Nothing -> pure $ RspNormal { resp = encodeNullArray, cmdName = "XRANGE" }
    Just seq -> xrangeCommand key mili seq
xrangeCommand key RangeMinusPlus seq = do
  streams <- getStreams
  let (filteredStream, _, _) = getStream streams key (M.lookupMin . M.filterWithKey (\_ _ -> True))
  case filteredStream of
    Nothing -> pure $ RspNormal { resp = encodeNullArray, cmdName = "XRANGE" }
    Just (EntryId m v, _) -> xrangeCommand key (RangeEntryId m v) seq
xrangeCommand key (RangeMili mili) seq@(RangeEntryId seq1 seq2) = xrangeCommand key (RangeEntryId mili 0) seq
xrangeCommand key mili@(RangeEntryId _ _) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ RspNormal { resp = encodeSimpleError RErrXRangeIDNonExisting mempty, cmdName = "XRANGE" }
    Just (RangeEntryId _ newEnd) -> xrangeCommand key mili (RangeEntryId seq newEnd)
xrangeCommand key (RangeMili mili) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ RspNormal { resp = encodeSimpleError RErrXRangeIDNonExisting mempty, cmdName = "XRANGE" }
    Just (RangeEntryId _ newEnd) -> xrangeCommand key (RangeEntryId mili 0) (RangeEntryId seq newEnd)
xrangeCommand key mili@(RangeEntryId _ _) seq@(RangeEntryId _ _) = do
  streams <- getStreams
  pure $ xrangeHelper streams key (<) mili seq

normRangeEnd :: Streams -> BS.ByteString -> (EntryId -> [BSPair] -> Bool) -> Maybe RangeEntryId
normRangeEnd streams key f =
  let (filteredStream, _, _) = getStream streams key (M.lookupMax . M.filterWithKey f)
  in case filteredStream of
       Nothing -> Nothing
       Just (EntryId m v, _) -> Just (RangeEntryId m v)

type KeysIds = [(BS.ByteString, RangeEntryId)]
xreadEntries :: Streams -> KeysIds -> (Bool, KeysIds)
xreadEntries streams keyIds = (hasEntries, normalizedIds)
  where
    normalizedIds = map (normalizeEntryId streams) keyIds
    hasEntries = any (streamHasEntries streams) normalizedIds

    normalizeEntryId :: Streams -> (BS.ByteString, RangeEntryId) -> (BS.ByteString, RangeEntryId)
    normalizeEntryId streams (key, entryID) =
      case entryID of
        RangeDollar -> do
          let respRange = normRangeEnd streams key (\_ _ -> True)
          case respRange of
            Just endId -> (key, endId)
            Nothing -> (key, RangeEntryId 0 0)
        RangeMili mili -> (key, RangeEntryId mili 0)
        RangeMinusPlus -> (key, RangeEntryId 0 0)
        _ -> (key, entryID)

    streamHasEntries :: Streams -> (BS.ByteString, RangeEntryId) -> Bool
    streamHasEntries streams (streamID, entryID) =
      case normRangeEnd streams streamID (\_ _ -> True) of
        Nothing -> False
        Just endID ->
          case xrangeHelper streams streamID (<=) entryID endID of
            (RspNormal { resp = streamResp }) -> streamResp /= "*0\r\n"
            _ -> False

applyReadCommand :: Streams -> KeysIds -> Response
applyReadCommand streams keysIds =
  let streamResponses = go streams keysIds []
  in if null streamResponses
       then RspNormal { resp = encodeNullArray, cmdName = "XREAD" }
       else RspNormal { resp = "*" <> (BS8.pack . show . length) streamResponses <> "\r\n" <> BS.concat streamResponses, cmdName = "XREAD" }
  where
    go :: Streams -> KeysIds -> [BS.ByteString] -> [BS.ByteString]
    go streams [] acc = reverse acc
    go streams ((stream_id, entry_id) : xs) acc =
      let res = normRangeEnd streams stream_id (\_ _ -> True)
      in case res of
           Nothing -> go streams xs acc
           Just seq -> let (RspNormal { resp = streamResp }) = xrangeHelper streams stream_id (<=) entry_id seq
                       in if streamResp == "*0\r\n"
                          then go streams xs acc
                          else go streams xs (("*2\r\n" <> encodeBulkString stream_id <> streamResp) : acc)

xreadCommand :: KeysIds -> Maybe Double -> ClientApp Response
xreadCommand keysIds (Just timeout) = do
  tvStreams <- getTVStreams
  streams <- getStreams
  let (hasEntries, normalizedIds) = xreadEntries streams keysIds
  if hasEntries
    then pure $ applyReadCommand streams normalizedIds
    else do
      toResp <- liftIO $ awaitWithTimeout (round (timeout * 1_000)) $ waitAction tvStreams normalizedIds
      case toResp of
        Nothing -> pure $ RspNormal { resp = encodeNullArray, cmdName = "XREAD" }
        Just resp -> pure resp
  where
    waitAction :: TVar Streams -> KeysIds -> STM Response
    waitAction tvStore ids = do
      storeData <- readTVar tvStore
      let (hasEntries, _) = xreadEntries storeData ids
      check hasEntries
      pure $ applyReadCommand storeData ids
xreadCommand keysIds Nothing = do
  streams <- getStreams
  let (_, normalizedIds) = xreadEntries streams keysIds
  pure $ applyReadCommand streams normalizedIds

incrCommand :: (MonadStore m) => BS.ByteString -> m Response
incrCommand key = do
  tv <- getData
  incrResult <- liftIO . atomically $ do
    curr <- readTVar tv
    case M.lookup key curr of
      Nothing -> do
        let nextVal = 1
        writeTVar tv (M.insert key (StoreEntry (StoreString "1") Nothing) curr)
        pure $ Just nextVal
      Just (StoreEntry (StoreString v) Nothing) -> case U.bsToInt v of
        Nothing -> pure Nothing
        Just i -> do
          let nextVal = i + 1
          writeTVar tv (M.insert key (StoreEntry (StoreString $ BS8.pack $ show nextVal) Nothing) curr)
          pure $ Just nextVal
      _ -> pure Nothing

  case incrResult of
    Nothing -> pure $ RspNormal { resp = encodeSimpleError RErrIncrNotIntegerOrRange mempty, cmdName = "INCR" }
    Just nextVal -> pure $ RspContinue { resp = encodeInteger nextVal
                                       , effect = EffUpdateReplica ["INCR", key]
                                       , cmdName = "INCR" }

execCommand :: ClientApp Response
execCommand = do
  multi <- getMulti
  updateMulti False
  if multi
    then do
      ml <- getMultiList
      if null ml
        then pure $ RspNormal { resp = encodeArray True [], cmdName = "EXEC" }
        else do
          res <- go ml []
          resetMultiCommands
          pure $ RspNormal { resp = encodeArray False res, cmdName = "EXEC" }
    else pure $ RspNormal { resp = encodeSimpleError RErrExecNoMulti mempty, cmdName = "EXEC" }
  where go [] acc = pure acc
        go (x : xs) acc = do
           resp <- x
           case resp of
                (RspNormal { resp = resp }) -> go xs (acc ++ [resp])
                (RspContinue { resp = resp }) -> go xs (acc ++ [resp])

discardCommand :: ClientApp Response
discardCommand = do
  multi <- getMulti
  if multi
    then do
      updateMulti False
      resetMultiCommands
      pure $ RspNormal { resp = encodeSimpleString "OK", cmdName = "DISCARD" }
    else pure $ RspNormal { resp = encodeSimpleError RErrDiscardNoMulti mempty, cmdName = "DISCARD" }

handleMultiCmd :: ClientApp Response -> ClientApp Response
handleMultiCmd op = do
  multi <- getMulti
  if multi
    then do
      addMultiCommand op
      pure $ RspNormal { resp = encodeSimpleString "QUEUED", cmdName = "MultiCmdHandler" }
    else op

infoCommand :: InfoRequest -> ClientApp Response
infoCommand Replication = do
  role <- getRole
  case role of
    Master repID repOffset -> pure $ RspNormal { resp = masterResponse repID repOffset, cmdName = "INFO" }
    Slave roHost roPort -> pure $ RspNormal { resp = encodeBulkString "# Replication\nrole:slave", cmdName = "INFO" }
  where
    masterResponse rID repOS = encodeBulkString $ "# Replication\nrole:master\nmaster_replid:" <> BS8.pack rID <> "\nmaster_repl_offset:" <> (BS8.pack . show) repOS

replConfCommand :: ReplConfOptions -> ClientApp Response
replConfCommand (ListeningPort port) = do
  pure $ RspNormal { resp = encodeSimpleString "OK", cmdName = "REPLCONF listening port" }
replConfCommand (Capa capa) = do
  pure $ RspNormal { resp = encodeSimpleString "OK", cmdName = "REPLCONF Capa" }
replConfCommand (AckWith offset) = do
  serverOffset <- getReplicaSentOffset
  if offset == serverOffset
    then do
      tvComplete <- asks $ senvCompleteReplicaCount . cenvShared
      liftIO . atomically $ do
        current <- readTVar tvComplete
        writeTVar tvComplete $ current + 1
      current <- liftIO $ readTVarIO tvComplete
      pure $ RspNormal { resp = mempty, cmdName = "REPLCONF AckWith" } -- TODO: potential code smell with resp = mempty
    else pure $ RspNormal { resp = mempty, cmdName = "REPLCONF AckWith" }

psyncCommand :: PSyncRequest -> ClientApp Response
psyncCommand PSyncUnknown = do
  repl <- getClientReplication
  case repl of
    Master repID _ -> do
      _sock <- getSocket
      addReplica _sock
      pure $ RspContinue { resp = encodeSimpleString $ "FULLRESYNC " <> BS8.pack repID <> " 0", effect = EffSnapshot, cmdName = "PSYNC" }
    _ -> pure $ RspNormal { resp = mempty, cmdName = "PSYNC" } -- TODO: potential code smell? A slave will never get a PSync command from a client

sendAckCommand :: Socket -> Int -> TVar Int -> IO ()
sendAckCommand sock serverOffset tvCompleted = do
  SA.withAsync worker $ \a -> do
    SA.wait a
  where
    worker :: IO ()
    worker = do
      -- liftIO $ hPutStrLn stderr "Sending REPLCONF GETACK *"
      liftIO $ send sock $ encodeArray True ["REPLCONF", "GETACK", "*"]

awaitWithTimeout :: Int -> STM a -> IO (Maybe a)
awaitWithTimeout timeoutMs waitAction
  | timeoutMs == 0 = atomically (Just <$> waitAction)
  | otherwise = do
    fired <- registerDelay timeoutMs
    atomically $ (Just <$> waitAction) `orElse` (readTVar fired >>= check >> pure Nothing)
  where
    check True  = pure ()
    check False = retry

blpopCommand :: BS.ByteString -> Int -> Int -> ClientApp Response
blpopCommand key timeout clientID = do
  tvStore <- asks $ (.sData) . senvStore . cenvShared
  toResp <- liftIO $ awaitWithTimeout timeout $ waitAction key clientID tvStore
  case toResp of
    Nothing -> pure $ RspNormal { resp = encodeNullArray, cmdName = "BLPOP" }
    Just (RspNormal { resp = resp }) -> pure $ RspNormal { resp = resp, cmdName = "BLPOP" }
    Just (RspContinue { resp = r, effect = ef }) -> pure $ RspContinue { resp = r, effect = ef, cmdName = "BLPOP" }
  where
    waitAction :: BS.ByteString -> Int -> TVar StoreData -> STM Response
    waitAction key clientID tvStore = do
      val <- M.lookup key <$> readTVar tvStore
      case val of
        Nothing -> retry
        Just (StoreEntry (StoreList []) Nothing) -> retry
        Just (StoreEntry (StoreList (x : xs)) Nothing) -> applyLPopHelper tvStore key 1 >>= \case
          (RspContinue { resp = resp, effect = effect }) -> pure $ RspContinue { resp = encodeArray False [encodeBulkString key, resp], effect = effect, cmdName = "BLPOP" }
          (RspNormal { resp = resp }) -> pure $ RspNormal { resp = resp, cmdName = "BLPOP" }

waitCommand :: Int -> Int -> ClientApp Response
waitCommand reqReady timeout = do
  tv <- getReplicas
  replicas <- liftIO $ readTVarIO tv
  serverOffset <- getReplicaSentOffset
  tvComplete <- asks $ senvCompleteReplicaCount . cenvShared
  liftIO . atomically $ modifyTVar' tvComplete (const 0)

  sendAcknowledgements serverOffset tvComplete replicas
  currOffset <- getReplicaSentOffset
  if currOffset > 0
    then do
    tvCompRepCount<- asks $ senvCompleteReplicaCount . cenvShared
    tvReplicaSentOS <- asks $ senvReplicaSentOffset . cenvShared
    toResp <- liftIO $ awaitWithTimeout timeout $ waitAction tvCompRepCount tvReplicaSentOS
    case toResp of
      Nothing -> do
        liftIO . atomically $ do
          updateServer tvReplicaSentOS
        resCompRepCount <- liftIO $ readTVarIO tvCompRepCount
        pure $ RspNormal { resp = encodeInteger resCompRepCount, cmdName = "WAIT" }
      Just resp -> pure resp
    else pure $ RspNormal { resp = encodeInteger $ length replicas, cmdName = "WAIT" }
  where
    waitAction :: TVar Int -> TVar Int -> STM Response
    waitAction tvCompRepCount tvReplicaSentOS = do
      compRepCount <- readTVar tvCompRepCount
      if compRepCount >= reqReady
      then do
        updateServer tvReplicaSentOS
        pure $ RspNormal { resp = encodeInteger compRepCount, cmdName = "WAIT" }
      else retry
    updateServer :: TVar Int -> STM ()
    updateServer tv = do
      let encodedCommand = encodeArray True ["REPLCONF", "GETACK", "*"]
      currOffset <- readTVar tv
      modifyTVar' tv (const (currOffset + BS8.length encodedCommand) )
    sendAcknowledgements :: Int -> TVar Int -> [Socket] -> ClientApp ()
    sendAcknowledgements serverOS tvComplete [] = pure ()
    sendAcknowledgements serverOS tvComplete (x : xs) = do
      liftIO $ sendAckCommand x serverOS tvComplete
      sendAcknowledgements serverOS tvComplete xs

configCommand :: ConfigArgs -> ClientApp Response
configCommand ConfigGetDir = do
  dir <- asks $ cfgDir . ccfgShared . cenvConfig
  pure $ RspNormal { resp = encodeArray True ["dir", BS8.pack dir], cmdName = "CONFIG" }
configCommand ConfigGetFileName = do
  fileName <- asks $ cfgRDBFileName . ccfgShared . cenvConfig
  pure $ RspNormal { resp = encodeArray True ["dbfilename", BS8.pack fileName], cmdName = "CONFIG" }

-- TODO: Filtering is VERY inefficient, redo at some point
keysCommand :: BS.ByteString -> ClientApp Response
keysCommand "*" = do
  tvStore <- getData
  store <- liftIO $ readTVarIO tvStore
  let result = M.keys store
  pure $ RspNormal { resp = encodeArray True result, cmdName = "KEYS" }
keysCommand filter = do
  tvStore <- getData
  store <- liftIO $ readTVarIO tvStore
  let result = M.keys store
  pure $ RspNormal { resp = encodeArray True (foldl' foldme [] result), cmdName = "KEYS" }
  where
    foldme :: [BS.ByteString] -> BS.ByteString -> [BS.ByteString]
    foldme acc item = if go filter item then acc ++ [item] else acc
    go :: BS.ByteString -> BS.ByteString -> Bool
    go xs ys = case (BS8.uncons xs, BS8.uncons ys) of
                 (Nothing, Nothing) -> True
                 (Nothing, _) -> False
                 (Just ('*', _), Nothing) -> True
                 (Just (x, _), Nothing) -> False
                 (Just (x, xs), Just (y, ys))
                   | x == '*' -> True
                   | x /= y -> False
                   | otherwise -> go xs ys

subscribeCommand :: BS.ByteString -> ClientApp Response
subscribeCommand channel = do
  socket <- getSocket
  setSubscribed True
  addSubChannel channel
  currChannels <- getSubChannels
  addChannelSubcriber channel socket
  pure $ RspSubs $ encodeArray False [encodeBulkString "subscribe", encodeBulkString channel, encodeInteger $ length currChannels]

publishCommand :: BS.ByteString -> BS.ByteString -> ClientApp Response
publishCommand channel msg = do
  clientList <- getChannelClients channel
  pure $ RspContinueSubs { resp = encodeInteger (length clientList), effect = EffPublishChannel channel msg }

zaddCommand :: BS.ByteString -> Double -> BS.ByteString -> ClientApp Response
zaddCommand name score member = do
  oldCount <- getZSetMemberCount name member
  addMemberToZSet name score member
  currCount <- getZSetMemberCount name member
  pure $ RspNormal { resp = encodeInteger $ currCount - oldCount, cmdName = "ZADD" }

zrankCommand :: BS.ByteString -> BS.ByteString -> ClientApp Response
zrankCommand name member = do
  (ZSet scoreMap memberDict) <- getZSet name
  maybeVal <- runMaybeT $ do
    score <- hoistMaybe $ HM.lookup member memberDict
    let precedingSets = M.elems (fst (M.split score scoreMap))
    let precedingCount = (sum . map S.size) precedingSets
    memberSet <- hoistMaybe $ M.lookup score scoreMap
    rank <- hoistMaybe $ S.lookupIndex member memberSet
    pure $ RspNormal { resp = encodeInteger (precedingCount + rank), cmdName = "ZRANK" }
  case maybeVal of
    Just result -> pure result
    Nothing -> pure $ RspNormal { resp = encodeNullBulkString, cmdName = "ZRANK" }

zrangeCommand :: BS.ByteString -> Int -> Int -> ClientApp Response
zrangeCommand name start end = do
  (ZSet scoreMap _) <- getZSet name
  let allScores = M.elems scoreMap
  let allScoresList = concatMap S.toAscList allScores
  let itemCount = length allScoresList
  let nStart = normStart start itemCount
  let nEnd = normStop end itemCount
  pure $ RspNormal { resp = encodeArray True (take (nEnd - nStart + 1) $ drop nStart allScoresList), cmdName = "ZRANGE" }
  where
    normStart index itemCount
      | index >= 0 = index
      | -index > itemCount = 0
      | otherwise = itemCount + index
    normStop index itemCount
      | index >= 0 = if index >= itemCount then itemCount - 1 else index
      | -index > itemCount = 0
      | otherwise = itemCount + index

zcardCommand :: BS.ByteString -> ClientApp Response
zcardCommand name = do
  (ZSet scoreMap _) <- getZSet name
  pure $ RspNormal { resp = encodeInteger $ (sum . map S.size) $ M.elems scoreMap, cmdName = "ZCARD" }

zscoreCommand :: BS.ByteString -> BS.ByteString -> ClientApp Response
zscoreCommand name member = do
  (ZSet _ memberDict) <- getZSet name
  case HM.lookup member memberDict of
    Just score -> pure $ RspNormal { resp = encodeBulkString (BS8.pack $ show score), cmdName = "ZSCORE" }
    Nothing -> pure $ RspNormal { resp = encodeNullBulkString, cmdName = "ZSCORE" }

zremCommand :: BS.ByteString -> BS.ByteString -> ClientApp Response
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
              pure $ RspNormal { resp = encodeInteger 1, cmdName = "ZREM" }
            Nothing -> pure $ RspNormal { resp = encodeInteger 0, cmdName = "ZREM" }
    Nothing -> pure $ RspNormal { resp = encodeInteger 0, cmdName = "ZREM" }

geoAddCommand :: BS.ByteString -> Double -> Double -> BS.ByteString -> ClientApp Response
geoAddCommand name longitude latitude member = do
  if longitude >= 180 || longitude <= -180
    then pure $ RspNormal { resp = encodeSimpleError RErrGeoAddLongRange mempty, cmdName = "GEOADD" }
    else
      if latitude >= 85.05112878 || latitude <= -85.05112878
        then pure $ RspNormal { resp = encodeSimpleError RErrGeoAddLatRange mempty, cmdName = "GEOADD" }
        else do
          zaddCommand name (U.interleaveGeo latitude longitude) member >>= \case
            (RspNormal { resp = resp }) -> pure $ RspNormal { resp = encodeInteger 1, cmdName = "GEOADD" }

geoPosCommand :: BS.ByteString -> [BS.ByteString] -> ClientApp Response
geoPosCommand name members = do
  (ZSet _ memberDict) <- getZSet name
  let values = map (`HM.lookup` memberDict) members
  pure $ RspNormal { resp = encodeArray False (go values []), cmdName = "GEOPOS" }
  where
    go :: [Maybe Double] -> [BS.ByteString] -> [BS.ByteString]
    go [] acc = acc
    go (x : xs) acc = do
      case x of
        Just score ->
          let (lat, long) = U.deinterleaveGeo score
           in go xs $ acc ++ [encodeArray True [(BS8.pack . show) long, (BS8.pack . show) lat]]
        Nothing -> go xs $ acc ++ [encodeNullArray]

geoDistCommand :: BS.ByteString -> BS.ByteString -> BS.ByteString -> ClientApp Response
geoDistCommand name member1 member2 = do
  (ZSet _ memberDict) <- getZSet name
  result <- runMaybeT $ do
    score1 <- hoistMaybe $ HM.lookup member1 memberDict
    score2 <- hoistMaybe $ HM.lookup member2 memberDict
    let (lat1, long1) = U.deinterleaveGeo score1
    let (lat2, long2) = U.deinterleaveGeo score2
    pure $ U.calcGeoDistance (long1, lat1) (long2, lat2)
  case result of
    Just dist -> pure $ RspNormal { resp = encodeBulkString ((BS8.pack . show) dist), cmdName = "GEODIST" }
    Nothing -> pure $ RspNormal { resp = encodeSimpleError RErrGeoDistMissingMember mempty, cmdName = "GEODIST" }

geoSearchCommand :: BS.ByteString -> Double -> Double -> Double -> DistUnit -> ClientApp Response
geoSearchCommand name longitude latitude radius unit = do
  (ZSet _ memberDict) <- getZSet name
  let normRadius = case unit of
        DistKilometer -> radius * 1_000
        DistMile -> radius * 1609.344
        _ -> radius
  let membersInRange = HM.foldrWithKey' (\member score acc -> if calcDistance score longitude latitude <= normRadius then acc ++ [member] else acc) [] memberDict
  pure $ RspNormal { resp = encodeArray True membersInRange, cmdName = "GEOSEARCH" }
  where
    calcDistance score centerLong centerLat =
      let (lat1, long1) = U.deinterleaveGeo score
       in U.calcGeoDistance (long1, lat1) (centerLong, centerLat)

aclCommand :: AclSubCmd -> ClientApp Response
aclCommand opt =
  case opt of
    AclWhoAmI -> do
      UserData {name = n} <- gets userData
      pure $ RspNormal { resp = encodeBulkString n, cmdName = "ACL" }
    AclGetUser user -> do
      UserData {flags = fl, passwords = ps} <- gets userData
      let response = [encodeBulkString "flags", encodeArray True fl, encodeBulkString "passwords", encodeArray True ps]
      pure $ RspNormal { resp = encodeArray False response, cmdName = "ACL" }
    AclSetUser user password -> do
      UserData {flags = fl, passwords = ps} <- gets userData
      let newFlags = delete "nopass" fl
      let encryptedPass = (B16.encode . SHA256.hash) password
      let newPasswords = encryptedPass : ps

      -- this client is now authenticated
      modify' (\cs -> cs {isAuth = True})

      -- Update that this server now requires authentication
      tv <- asks $ senvIsAuth . cenvShared
      tvUserPass <- asks $ senvAuthUsers . cenvShared
      liftIO . atomically $ do
        modifyTVar' tv (const True)
        modifyTVar' tvUserPass (HM.insert user encryptedPass)

      modify' (\cs -> cs {userData = UserData {name = user, passwords = newPasswords, flags = newFlags}})
      pure $ RspNormal { resp = encodeSimpleString "OK", cmdName = "ACL" }

authCommand :: BS.ByteString -> BS.ByteString -> ClientApp Response
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
            pure $ RspNormal { resp = encodeSimpleString "OK", cmdName = "AUTH" }
          else pure $ RspNormal { resp = encodeSimpleError RErrAuthInvalidUserName mempty, cmdName = "AUTH" }
      Nothing -> pure $ RspNormal { resp = encodeSimpleError RErrAuthServerAuthUserNotFound mempty, cmdName = "AUTH" }
    else do
      if user == userName && elem encodedPass ps
        then do
          modify' (\cs -> cs {isAuth = True}) -- this client is now authcenticated
          pure $ RspNormal { resp = encodeSimpleString "OK", cmdName = "AUTH" }
        else do
          pure $ RspNormal { resp = encodeSimpleError RErrAuthInvalidUserName mempty, cmdName = "AUTH" }

isAuthorizedCmd :: Command -> ClientApp Bool
isAuthorizedCmd cmd = do
  tvServerAuth <- asks $ senvIsAuth . cenvShared
  isServerAuth <- liftIO $ readTVarIO tvServerAuth
  isUserAuth <- gets isAuth
  isAuthCommand <- case cmd of Auth {} -> pure True; _ -> pure False
  pure $ (isServerAuth && (isUserAuth || isAuthCommand)) || not isServerAuth

handleSubsMode :: Response -> ClientApp ()
handleSubsMode resp = do
  sock <- getSocket
  subMode <- getSubscribed
  if subMode then
    case resp of
      (RspNormal { cmdName = cn }) -> liftIO $ send sock $ encodeSimpleError RErrSubUnauthorizedCmd cn
      (RspSubs resp) -> liftIO $ send sock resp
      RspPing -> liftIO $ send sock $ encodeArray True ["pong", ""]
      (RspContinue { cmdName = cn }) -> liftIO $ send sock $ encodeSimpleError RErrSubUnauthorizedCmd cn
      (RspContinueSubs { resp = r, effect = ef }) -> liftIO (send sock r) >> applyEffect ef
  else case resp of
      (RspNormal { resp = r }) -> liftIO $ send sock r
      (RspContinue { resp = r, effect = ef }) -> liftIO (send sock r) >> applyEffect ef
      (RspSubs resp) -> liftIO (send sock resp)
      (RspContinueSubs { resp = r }) -> liftIO (send sock r)
      RspPing -> liftIO (send sock $ encodeSimpleString "PONG")

unsubcribeCommand :: BS.ByteString -> ClientApp Response
unsubcribeCommand channel = do
  socket <- getSocket
  setSubscribed False
  removeSubChannel channel
  removeChannelSubscriber channel socket
  remaining <- getSubChannels
  pure $ RspSubs $ encodeArray False [encodeBulkString "unsubscribe", encodeBulkString channel, encodeInteger (S.size remaining)]

runClientCommand :: Command -> ClientApp ()
runClientCommand cmd = do
  isAuth <- isAuthorizedCmd cmd
  sock <- getSocket
  if isAuth then do
     cmdResp <- case cmd of
             Ping -> handleMultiCmd $ pure RspPing
             (Echo str) -> handleMultiCmd $ pure $ RspNormal { resp = encodeBulkString str, cmdName = "ECHO" }
             (Set key val args) -> handleMultiCmd $ setCommand key val args
             (Get key) -> handleMultiCmd $ getCommand key
             (RPush key values) -> handleMultiCmd $ pushCommand key values RightPushCmd
             (LPush key values) -> handleMultiCmd $ pushCommand key values LeftPushCmd
             (LRange key start stop) -> handleMultiCmd $ lrangeCommand key start stop
             (LLen key) -> handleMultiCmd $ llenCommand key
             (LPop key count) -> handleMultiCmd $ lpopCommand key count
             (BLPop key timeout) -> getClientID >>= \clientID -> handleMultiCmd $ blpopCommand key (round (timeout * 1_000_000)) clientID
             (Type key) -> handleMultiCmd $ typeCommand key
             (XAdd streamID entryID values) -> handleMultiCmd $ xaddCommand streamID entryID values
             (XRange key start end) -> handleMultiCmd $ xrangeCommand key start end
             (XRead keysIds timeout) -> handleMultiCmd $ xreadCommand keysIds timeout
             (Incr key) -> handleMultiCmd $ incrCommand key
             Multi -> updateMulti True >> (pure $ RspNormal { resp = encodeSimpleString "OK", cmdName = "MULTI" })
             Exec -> execCommand
             Discard -> discardCommand
             (Info infoRequest) -> handleMultiCmd $ infoCommand infoRequest
             (ReplConf replOptions) -> replConfCommand replOptions
             (Psync req) -> psyncCommand req
             (Wait replicaNum timeout) -> waitCommand replicaNum (round (timeout * 1_000))
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
             Cmd -> pure $ RspSubs encodeEmptyArray -- convenience command for running redis-cli in bulk mode
     handleSubsMode cmdResp
  else liftIO $ send sock (encodeSimpleError RErrAuthRequired mempty)
