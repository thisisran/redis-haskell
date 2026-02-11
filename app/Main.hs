{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import System.FilePath ((</>))
import System.Environment (getArgs)
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout, withBinaryFile, IOMode(ReadMode), Handle, SeekMode(RelativeSeek), hSeek)

import qualified Control.Concurrent.Async as SA
-- import Control.Exception (IOException)
import qualified Control.Exception as CE
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Monad (when, unless, void)

import Control.Monad.State.Strict (gets)
import Control.Monad.Reader (asks)

import UnliftIO (MonadUnliftIO, withRunInIO)
import qualified UnliftIO as UL

import RDBParser

import Data.List (foldl')
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
setCommand :: MonadStore m => BS.ByteString -> BS.ByteString -> Maybe SetExpiry -> m Response
setCommand key val ex = do
  now <- liftIO U.nowNs
  setDataEntry key $ handleExpiry ex now

  let exCommand = case ex of
                   Nothing -> []
                   Just (EX n) -> ["ex", (BS8.pack . show) n]
                   Just (PX n) -> ["px", (BS8.pack . show) n]
  pure $ Response (encodeSimpleString "OK") (updateReplicas $ ["SET", key, val] ++ exCommand)
  where
    handleExpiry Nothing timeRef = MemoryStoreEntry (MSStringVal val) Nothing
    handleExpiry (Just (EX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (ExpireDuration (fromIntegral $ ex * 1_000_000), ExpireReference timeRef) -- TODO: shouldn't it be multiplied by a 1000??
    handleExpiry (Just (PX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (ExpireDuration (fromIntegral ex), ExpireReference timeRef)

getCommand :: BS.ByteString -> ClientApp Response
getCommand key = do
  tv <- getDataEntry key
  case tv of
    Nothing -> pure $ Response encodeNullBulkString emptyResponse
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> pure $ Response (encodeBulkString v) emptyResponse
    Just (MemoryStoreEntry (MSStringVal v) (Just (ExpireDuration exDur, ExpireReference exRef))) -> do
      hasPassed <- liftIO $ U.hasElapsedSince exDur exRef
      if hasPassed
        then do
          delDataEntry key
          pure $ Response encodeNullBulkString emptyResponse
        else pure $ Response (encodeBulkString v) emptyResponse

pushCommand :: MonadStore m => BS.ByteString -> [BS.ByteString] -> PushCommand -> m Response
pushCommand key values pushType = do
  val <- getDataEntry key
  let valuesCount = length values
  let pushArgs = case pushType of
                   RightPushCmd -> "RPUSH"
                   LeftPushCmd  -> "LPUSH"
  case val of
    Nothing -> do
      setDataEntry key (MemoryStoreEntry (MSListVal $ newItems pushType) Nothing)
      pure $ Response (encodeInteger valuesCount) (updateReplicas $ [pushArgs, key] ++ values)
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      setDataEntry key (MemoryStoreEntry (MSListVal $ newList pushType vs) Nothing)
      pure $ Response (encodeInteger (length vs + valuesCount)) (updateReplicas $ [pushArgs, key] ++ values)
  where
    newItems RightPushCmd = values
    newItems LeftPushCmd = reverse values
    newList RightPushCmd oldList = oldList ++ values
    newList LeftPushCmd oldList = reverse values ++ oldList

lrangeCommand :: BS.ByteString -> Int -> Int -> ClientApp Response
lrangeCommand key start stop = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ Response (encodeArray True []) emptyResponse
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      pure $ Response (encodeArray True $ go vs (normStart start) (normStop stop)) emptyResponse
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
    Nothing -> pure $ Response (encodeInteger 0) emptyResponse
    Just (MemoryStoreEntry (MSListVal v) Nothing) -> pure $ Response (encodeInteger $ length v) emptyResponse

applyLPopHelper :: MonadStore m => BS.ByteString -> Int -> m Response
applyLPopHelper key count = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ Response encodeNullBulkString (updateReplicas ["LPOP", key, (BS8.pack . show) count])
    Just (MemoryStoreEntry (MSListVal v) Nothing) ->
      let normCount = min count $ length v
      in go v normCount
         where
           go [] _ = pure $ Response encodeNullBulkString (updateReplicas ["LPOP", key, (BS8.pack . show) count])
           go xs c = do
             setDataEntry key (MemoryStoreEntry (MSListVal (drop c xs)) Nothing)
             pure $ Response (getPopped c v) (updateReplicas ["LPOP", key, (BS8.pack . show) count])
             where
               getPopped 1 (x : _) = encodeBulkString x
               getPopped popCount xs = encodeArray True (take popCount xs)

lpopCommand :: BS.ByteString -> Int -> ClientApp Response
lpopCommand = applyLPopHelper

execWithTimeout :: Double -> ClientApp Response -> ClientApp (Bool, Response) -> ClientApp Response
execWithTimeout timeout timeoutOp otherOp = go 0
  where
    go :: Double -> ClientApp Response
    go elapsed
      | elapsed > timeout && timeout > 0 = timeoutOp
      | otherwise = do
          (shouldContinue, response) <- otherOp
          if shouldContinue then do
            liftIO $ threadDelay 1_000
            go $ elapsed + 0.001
          else pure response

blpopCommand :: BS.ByteString -> Double -> Int -> ClientApp Response
blpopCommand key timeout clientID = do
  liftIO $ hPutStrLn stderr ("Starting BLPop with timeout: " <> show timeout)
  execWithTimeout timeout noTimeOp mainOp
  where noTimeOp = pure $ Response encodeNullArray emptyResponse
        mainOp = do
          val <- getDataEntry key
          case val of
            Nothing -> do
              addWaiterOnce key clientID
              pure (True, Response "" (pure ()))
            Just (MemoryStoreEntry (MSListVal []) Nothing) -> do
              addWaiterOnce key clientID
              pure (True, Response "" (pure ()))
            Just (MemoryStoreEntry (MSListVal (x : xs)) Nothing) -> do
              waiters <- getWaiterEntry key
              case waiters of
                Nothing -> do
                  (Response resp _) <- applyLPopHelper key 1
                  pure (False, Response (encodeArray False [encodeBulkString key, resp]) emptyResponse)
                Just waitersList -> do
                  if IS.member clientID waitersList
                    then do
                      (Response resp _) <- applyLPopHelper key 1
                      delWaiterEntry key
                      pure (False, Response (encodeArray False [encodeBulkString key, resp]) emptyResponse)
                    else do
                      addWaiterOnce key clientID
                      pure (True, Response "" (pure ()))

typeCommand :: BS.ByteString -> ClientApp Response
typeCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      (Streams streams) <- getStreams
      case HM.lookup key streams of
        Just _ -> pure $ Response (encodeSimpleString "stream") emptyResponse
        Nothing -> pure $ Response (encodeSimpleString "none") emptyResponse
    Just (MemoryStoreEntry (MSStringVal _) _) -> pure $ Response (encodeSimpleString "string") emptyResponse
    Just (MemoryStoreEntry (MSListVal _) _) -> pure $ Response (encodeSimpleString "list") emptyResponse

xaddCommand :: MonadStore m => BS.ByteString -> EntryId -> RedisStreamValues -> m Response
xaddCommand streamID (EntryId 0 0) values = pure $ Response (encodeSimpleError "The ID specified in XADD must be greater than 0-0") emptyResponse
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
        then pure $ Response (encodeSimpleError "The ID specified in XADD is equal or smaller than the target stream top item") (updateReplicas replicaRep)
        else addNewEntry oldStream streams
  where
    addNewEntry :: MonadStore m => M.Map EntryId RedisStreamValues -> RedisStreams -> m Response
    addNewEntry oldStream (Streams streams) = do
      let newEntry = values
      let newStream = Stream (M.insert entryID newEntry oldStream)
      let newStreams = HM.insert streamID newStream streams
      setStreams (MemoryStoreEntry (MSStreams (Streams newStreams)) Nothing)
      pure $ Response (encodeBulkString (U.entryIdToBS entryID)) emptyResponse
    valuesToArray [] acc = acc
    valuesToArray ((key,value):xs) acc = valuesToArray xs (acc ++ [key, value])

xrangeEndHelper :: BS.ByteString -> (EntryId -> RedisStreamValues -> Bool) -> ClientApp (Maybe RangeEntryId)
xrangeEndHelper key f = do
  (filteredStream, _, streams) <- getStream key (M.lookupMax . M.filterWithKey f)
  case filteredStream of
    Nothing -> pure Nothing
    Just (EntryId m v, _) -> pure $ Just (RangeEntryId m v)

xrangeHelper :: BS.ByteString -> (EntryId -> EntryId -> Bool) -> RangeEntryId -> RangeEntryId -> ClientApp Response
xrangeHelper key rangef (RangeEntryId mili1 mili2) (RangeEntryId seq1 seq2) = do
  (_, Stream oldStream, _) <- getStream key (const Nothing . M.filterWithKey (\_ _ -> True))
  -- putStrLn $ show ((M.takeWhileAntitone (<= (EntryId 1526985054079 0)) $ M.dropWhileAntitone ((<=) (EntryId 1526985054069 0)) oldStream))
  let allKeysValues = M.toAscList (U.range rangef (EntryId mili1 mili2) (EntryId seq1 seq2) oldStream)
  let resp = parseKeysValues allKeysValues
  pure $ Response resp emptyResponse
  where
    parseKeysValues :: [(EntryId, RedisStreamValues)] -> BS.ByteString
    parseKeysValues keysValues =
      "*"
        <> (BS8.pack . show . length) keysValues
        <> "\r\n"
        <> foldr (\(id, valuesMap) acc -> "*2\r\n" <> encodeBulkString (U.entryIdToBS id) <> convertMap valuesMap <> acc) BS.empty keysValues
    convertMap :: RedisStreamValues -> BS.ByteString
    convertMap l =
      "*"
        <> (BS8.pack . show . (* 2) . length) l
        <> "\r\n"
        <> foldr (\(k, v) acc -> encodeBulkString k <> encodeBulkString v <> acc) BS.empty l

xrangeCommand :: BS.ByteString -> RangeEntryId -> RangeEntryId -> ClientApp Response
xrangeCommand key mili RangeMinusPlus = do
  res <- xrangeEndHelper key (\_ _ -> True)
  case res of
    Nothing -> pure $ Response encodeNullArray emptyResponse
    Just seq -> xrangeCommand key mili seq
xrangeCommand key RangeMinusPlus seq = do
  (filteredStream, Stream oldStream, streams) <- getStream key (M.lookupMin . M.filterWithKey (\_ _ -> True))
  case filteredStream of
    Nothing -> pure $ Response encodeNullArray emptyResponse
    Just (EntryId m v, _) -> xrangeCommand key (RangeEntryId m v) seq
xrangeCommand key (RangeMili mili) seq@(RangeEntryId seq1 seq2) = xrangeCommand key (RangeEntryId mili 0) seq
xrangeCommand key mili@(RangeEntryId _ _) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ Response (encodeSimpleError "XRANGE: The end id does not exist") emptyResponse
    Just (RangeEntryId _ newEnd) -> xrangeCommand key mili (RangeEntryId seq newEnd)
xrangeCommand key (RangeMili mili) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ Response (encodeSimpleError "XRANGE: The end id does not exist") emptyResponse
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

xreadCommand :: [(BS.ByteString, RangeEntryId)] -> Maybe Double -> ClientApp Response
xreadCommand keysIds (Just timeout) = go 0 keysIds
  where
    go elapsed ids = do
      (hasEntries, newIds) <- xreadEntriesAvailable ids
      if hasEntries
        then xreadCommand newIds Nothing
        else
          if elapsed > (timeout / 1_000) && timeout > 0
            then pure $ Response encodeNullArray emptyResponse
            else do
              liftIO $ threadDelay 1_000
              go (elapsed + 0.001) newIds
xreadCommand keysIds Nothing = do
  result <- go keysIds BS.empty 0
  pure $ Response ("*" <> (BS8.pack . show . length) keysIds <> "\r\n" <> result) emptyResponse
  where
    go :: [(BS.ByteString, RangeEntryId)] -> BS.ByteString -> Double -> ClientApp BS.ByteString
    go [] acc elapsed = pure acc
    go ((stream_id, entry_id) : xs) acc elapsed = do
      res <- xrangeEndHelper stream_id (\_ _ -> True)
      case res of
        Nothing -> pure encodeNullArray
        Just seq -> do
          (Response streamResp _) <- xrangeHelper stream_id (<=) entry_id seq
          go xs (acc <> "*2\r\n" <> encodeBulkString stream_id <> streamResp) elapsed

incrCommand :: MonadStore m => BS.ByteString -> m Response
incrCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      setDataEntry key (MemoryStoreEntry (MSStringVal "1") Nothing)
      pure $ Response (encodeInteger 1) (updateReplicas ["INCR", key])
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> case U.bsToInt v of
      Nothing -> pure $ Response (encodeSimpleError "value is not an integer or out of range") emptyResponse
      Just i -> do
        setDataEntry key (MemoryStoreEntry (MSStringVal $ (BS8.pack . show) (i + 1)) Nothing)
        pure $ Response (encodeInteger $ i + 1) (updateReplicas ["INCR", key])

execCommand :: ClientApp Response
execCommand = do
  multi <- getMulti
  updateMulti False
  if multi
  then do
    ml <- getMultiList
    if null ml
    then pure $ Response (encodeArray True []) emptyResponse
    else do
      ml <- getMultiList
      res <- go ml []
      pure $ Response (encodeArray False res) emptyResponse
  else pure $ Response (encodeSimpleError "EXEC without MULTI") emptyResponse
  where go :: [ClientApp Response] -> [BS.ByteString] -> ClientApp [BS.ByteString]
        go [] acc = pure acc
        go (x:xs) acc = do
           (Response resp _) <- x
           go xs (acc ++ [resp])

discardCommand :: ClientApp Response
discardCommand = do
  multi <- getMulti
  if multi
  then do
    updateMulti False
    resetMultiCommands
    pure $ Response (encodeSimpleString "OK") emptyResponse
  else pure $ Response (encodeSimpleError "DISCARD without MULTI") emptyResponse

handleMultiCmd :: ClientApp Response -> ClientApp Response
handleMultiCmd op = do
  multi <- getMulti
  if multi
  then do
    addMultiCommand op
    pure $ Response (encodeSimpleString "QUEUED") emptyResponse
  else op

infoCommand :: InfoRequest -> ClientApp Response
infoCommand Replication = do
  role <- getRole
  case role of
    Master repID repOffset -> pure $ Response (masterResponse repID repOffset) emptyResponse
    Slave roHost roPort -> pure $ Response (encodeBulkString "# Replication\nrole:slave") emptyResponse
  where masterResponse rID repOS = encodeBulkString $ "# Replication\nrole:master\nmaster_replid:" <> BS8.pack rID <> "\nmaster_repl_offset:" <> (BS8.pack . show) repOS

replConfCommand :: ReplConfOptions -> ClientApp Response
replConfCommand (ListeningPort port) = do
  pure $ Response (encodeSimpleString "OK") emptyResponse
replConfCommand (Capa capa) = do
  pure $ Response (encodeSimpleString "OK") emptyResponse
replConfCommand (AckWith offset) = do
  serverOffset <- getReplicaSentOffset
  if offset == serverOffset
  then do
    tvComplete <- asks $ senvCompleteReplicaCount . cenvShared
    liftIO . atomically $ do
      current <- readTVar tvComplete
      writeTVar tvComplete $ current + 1
    current <- liftIO $ readTVarIO tvComplete
    pure $ Response mempty emptyResponse
  else pure $ Response mempty emptyResponse

sendSnapshot :: ClientApp ()
sendSnapshot = do
  socket <- getSocket
  case U.decodeRdbBase64 U.emptyRdbFile of
    Right x -> liftIO $ send socket $ encodeRdbFile x

psyncCommand :: PSyncRequest -> ClientApp Response
psyncCommand PSyncUnknown = do
  repl <- getClientReplication
  case repl of
    Master repID _ -> do
      _sock <- getSocket
      addReplica _sock
      pure $ Response (encodeSimpleString $ "FULLRESYNC " <> BS8.pack repID <> " 0") sendSnapshot
    _ -> pure $ Response mempty emptyResponse -- A slave will never get a PSync command from a client

sendAckCommand :: Socket -> Int -> TVar Int -> IO ()
sendAckCommand sock serverOffset tvCompleted = do
  SA.withAsync worker $ \a -> do
    SA.wait a
  where
       worker :: IO ()
       worker = do
          -- liftIO $ hPutStrLn stderr "Sending REPLCONF GETACK *"
          liftIO $ send sock $ encodeArray True ["REPLCONF", "GETACK", "*"]

waitCommand :: Int -> Double -> ClientApp Response
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
  else pure $ Response (encodeInteger $ length replicas) emptyResponse

  where sendAcknowledgements serverOS tvComplete [] = pure ()
        sendAcknowledgements serverOS tvComplete (x:xs) = do
          liftIO $ sendAckCommand x serverOS tvComplete
          sendAcknowledgements serverOS tvComplete xs
        countSoFarOp :: ClientApp Response
        countSoFarOp = do
          result <- getCompleteReplicas
          -- liftIO $ hPutStrLn stderr ("WAIT Timedout, returning " <> show result)
          updateServerSent
          pure $ Response (encodeInteger result) emptyResponse
        countFullOp :: ClientApp (Bool, Response)
        countFullOp = do
          result <- getCompleteReplicas
          if result >= reqReady
          then do
            -- liftIO $ hPutStrLn stderr ("WAIT got enough requests, returning " <> show result)
            updateServerSent
            pure (False, Response (encodeInteger result) emptyResponse)
          else pure (True, Response "" emptyResponse)
        getCompleteReplicas = do
          tv <- asks (senvCompleteReplicaCount . cenvShared)
          liftIO $ readTVarIO tv
        updateServerSent = do
            let encodedCommand = encodeArray True ["REPLCONF", "GETACK", "*"]
            currOffset <- getReplicaSentOffset
            setReplicaSentOffset (BS8.length encodedCommand + currOffset)

configCommand :: ConfigArgs -> ClientApp Response
configCommand ConfigGetDir = do
  dir <- asks $ cfgDir . ccfgShared . cenvConfig
  pure $ Response (encodeArray True ["dir", BS8.pack dir]) emptyResponse
configCommand ConfigGetFileName = do
  fileName <- asks $ cfgRDBFileName . ccfgShared . cenvConfig
  pure $ Response (encodeArray True ["dbfilename", BS8.pack fileName]) emptyResponse

-- TODO: Filtering is VERY inefficient, redo at some point
keysCommand :: BS.ByteString -> ClientApp Response
keysCommand "*" = do
  tvStore <- getData
  store <- liftIO $ readTVarIO tvStore
  let result = M.keys store
  pure $ Response (encodeArray True result) emptyResponse
keysCommand filter = do
  tvStore <- getData
  store <- liftIO $ readTVarIO tvStore
  let result = M.keys store
  pure $ Response (encodeArray True (foldl' foldme [] result)) emptyResponse
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

subscribeCommand :: BS.ByteString -> ClientApp Response
subscribeCommand channel = do
  socket <- getSocket
  setSubscribed True
  addSubChannel channel
  currChannels <- getSubChannels
  addChannelSubcriber channel socket
  pure $ Response (encodeArray False [encodeBulkString "subscribe", encodeBulkString channel, encodeInteger $ length currChannels]) emptyResponse

publishCommand :: BS.ByteString -> BS.ByteString -> ClientApp Response
publishCommand channel msg = do
  socket <- getSocket
  subCount <- getChannelClientCount channel
  pure $ Response (encodeInteger subCount) emptyResponse

approvedSubCommand :: Command -> Bool
approvedSubCommand cmd = case cmd of
                           (Subscribe _) -> True
                           Ping          -> True
                           (Publish _ _) -> True
                           _             -> False

execSubCommand :: Command -> ClientApp ()
execSubCommand cmd = do
  socket <- getSocket
  case cmd of
    (Subscribe channel) -> do
      (Response resp _) <- subscribeCommand channel
      liftIO $ send socket resp
    Ping                -> liftIO $ send socket $ encodeArray True ["pong", ""]
    Publish channel msg -> do
      (Response resp _) <- publishCommand channel msg
      liftIO $ send socket resp

getCommandName :: Command -> BS.ByteString
getCommandName cmd = case cmd of
                       (Echo _) -> "Echo"
                       (Set _ _ _) -> "Set"
                       (Get _) -> "Get"
                       (RPush _ _) -> "RPush"
                       (LPush _ _) -> "LPush"
                       (LRange _ _ _) -> "LRange"
                       (LLen _) -> "LLen"
                       (LPop _ _) -> "LPop"
                       (BLPop _ _) -> "BLPop"
                       (Type _) -> "Type"
                       (XAdd _ _ _) -> "XAdd"
                       (XRange _ _ _) -> "XRange"
                       (XRead _ _) -> "XRead"
                       (Incr _) -> "Incr"
                       Multi -> "Multi"
                       Exec -> "Exec"
                       Discard -> "Discard"
                       (Info _) -> "Info"
                       (ReplConf _) -> "ReplConf"
                       (Psync _) -> "Psync"
                       (Wait _ timeout) -> "Wait"
                       (Config _) -> "Config"
                       (Keys _) -> "Keys"
                       

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
              subMode <- getSubscribed
              if subMode then do
                 if approvedSubCommand cmd
                 then execSubCommand cmd
                 else do
                   let commandName = getCommandName cmd
                   liftIO $ send sock $ encodeSimpleError ("Can't execute '" <> commandName <> "' when one or more subscriptions exist")
              else do
                Response resp nextAction <- case cmd of
                                            Ping -> handleMultiCmd $ pure $ Response (encodeSimpleString "PONG") emptyResponse
                                            (Echo str) -> handleMultiCmd $ pure $ Response (encodeBulkString str) emptyResponse
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
                                              pure $ Response (encodeSimpleString "OK") emptyResponse
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
                                            Cmd  -> pure $ Response "*0\r\n" emptyResponse -- convenience command for running redis-cli in bulk mode
                liftIO $ send sock resp
                nextAction
              go rest                -- rest may already contain next command
            RParserErr e -> do
              liftIO $ send sock e

---------------------------------------------------------------------------------------------------------
-- Slave functions

recvByParser :: (BS.ByteString -> StringParserResult) -> Socket -> BS.ByteString -> ReplicaApp TCPReceivedResult
recvByParser parser sock = go
  where
    go acc = do
      case parser acc of
        SParserFullString str rest -> pure $ TCPResultFull str rest
        SParserPartialString       -> recv sock 4096 >>= \case
          Nothing -> pure $ TCPResultError "Received only partial response"
          Just chunk -> go (acc <> chunk)
        SParserError msg           -> pure $ TCPResultError msg

recvSimpleResponse :: Socket -> BS.ByteString -> ReplicaApp TCPReceivedResult
recvSimpleResponse = recvByParser parseSimpleString

recvRDBFile :: Socket -> BS.ByteString -> ReplicaApp TCPReceivedResult
recvRDBFile = recvByParser parseRDBFile

getAckCommand :: ReplicaApp Response
getAckCommand = do
  offset <- getReplicaOffset
  pure $ Response (encodeArray True ["REPLCONF", "ACK", (BS8.pack . show) offset]) emptyResponse 

awaitServerUpdates :: Socket -> BS.ByteString -> ReplicaApp ()
awaitServerUpdates sock = go 0
  where
    go :: Int -> BS.ByteString -> ReplicaApp ()
    go offset buffered = do
      mb <- if BS.null buffered then liftIO (recv sock 4096) else pure (Just buffered)
      case mb of
        Nothing -> pure ()
        Just buf -> do
          setReplicaOffset offset
          case parseOneCommand buf of
            RParsed cmd rest -> do
              let processedCount = BS.length buf - BS.length rest
              case cmd of
                Set key val args        -> void $ setCommand key val args
                RPush key values        -> void $ pushCommand key values RightPushCmd
                LPush key values        -> void $ pushCommand key values LeftPushCmd
                LPop key count          -> void $ applyLPopHelper key count
                XAdd streamID entryID v -> void $ xaddCommand streamID entryID v
                Incr key                -> void $ incrCommand key
                ReplConf GetAck         -> do
                  getAckCommand >>= \(Response resp _) -> send sock resp
                _                       -> pure ()
              go (offset + processedCount) rest
            RParserNeedMore -> do
              mbMore <- liftIO $ recv sock 4096
              case mbMore of
                Nothing   -> pure () -- TODO: proper error reporting/propagaton, Server closed the connection for some reason
                Just more -> go offset (buf <> more)
            RParserErr _ -> pure () -- TODO: proper error reporting/propagaton

runReplica :: ReplicaApp ()
runReplica = do
  clientPort <- getPort
  getReplication >>= \case
    Master _ _ -> pure mempty
    -- withRunInIO :: ((ReplicaApp () -> IO ()) -> IO) -> ReplicaApp()
    Slave host port -> withRunInIO $ \runInIO -> do
      putStrLn ("Trying to connect to " <> host <> " " <> port)
      _ <- UL.async $ connect host port $ \(_sock, _addr) ->
        -- runInIO :: ReplicaApp () -> IO ()
        runInIO $ do
          send _sock $ encodeArray True ["PING"]
          respPing <- recvSimpleResponse _sock mempty
          case respPing of
            TCPResultFull pongResp pending0 ->
              case pongResp of
                "PONG" -> do
                  liftIO $ send _sock $ encodeArray True ["REPLCONF", "listening-port", BS8.pack clientPort]
                  respReplConf1 <- recvSimpleResponse _sock pending0
                  case respReplConf1 of
                    TCPResultFull replResp1 pending1 ->
                      case replResp1 of
                        "OK" -> do
                          liftIO $ send _sock $ encodeArray True ["REPLCONF", "capa", "psync2"]
                          respReplConf2 <- recvSimpleResponse _sock pending1
                          case respReplConf2 of
                            TCPResultFull replResp2 pending2 ->
                              case replResp2 of
                                "OK" -> do
                                  liftIO $ send _sock $ encodeArray True ["PSYNC", "?", "-1"]
                                  -- TODOHERE
                                  respPsync <- recvSimpleResponse _sock pending2
                                  case respPsync of
                                    TCPResultFull replPsync pending3 -> do
                                        -- TODO: parse replPsync to get the replica ID and the offset
                                        -- TODO: parse Rdb file response and update the DB accordingly
                                        respRDBFile <- recvRDBFile _sock pending3
                                        case respRDBFile of
                                          TCPResultFull rdbFile pending4 -> awaitServerUpdates _sock pending4
                                          TCPResultError err -> liftIO $ hPutStrLn stderr ("Replica mode: " <> BS8.unpack err) 
                                    TCPResultError err -> liftIO $ hPutStrLn stderr ("Replica mode: " <> BS8.unpack err)
                                _ -> liftIO $ hPutStrLn stderr "Replica mode: failed hankshake, did not receive confirmation for REPLCONF capa psync2"
                            TCPResultError err -> liftIO $ hPutStrLn stderr ("Replica mode: " <> BS8.unpack err)
                        _ -> liftIO $ hPutStrLn stderr "Replica mode: failed hankshake, did not receive confirmation for REPLCONF listening-port"
                    TCPResultError err -> liftIO $ hPutStrLn stderr ("Replica mode: " <> BS8.unpack err)
                _ -> liftIO $ hPutStrLn stderr "Replica mode: failed hankshake, did not receive reponse to PING"
            TCPResultError err -> liftIO $ hPutStrLn stderr ("Replica mode: " <> BS8.unpack err)
      pure ()

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

  newReplicas <- newTVarIO []
  sentOffset <- newTVarIO 0
  complReplicas <- newTVarIO 0
  newChannels <- newTVarIO HM.empty
  let sharedEnv = SharedEnv store sharedCfg newReplicas sentOffset complReplicas newChannels

  newReplicaOffset <- newTVarIO 0
  let replicaEnv = ReplicaEnv sharedEnv newReplicaOffset

  case sharedCfg of
      SharedConfig _ _ _ (Slave _ _) -> runReplicaApp replicaEnv runReplica
      _ -> pure ()

  putStrLn $ "Redis server listening on port " ++ port
  serve HostAny port $ \(socket, address) -> do
    putStrLn $ "successfully connected client: " ++ show address

    clientID <- atomically $ do
      i <- readTVar nextID
      writeTVar nextID (i + 1)
      pure i

    let cs = ClientState {multi = False, multiList = [], subscribeMode = False, subscribeChannels = S.empty }

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
