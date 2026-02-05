{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import Data.Maybe (fromMaybe)
import Data.Word (Word64)
import Encode (encodeArray, encodeBulkString, encodeInteger, encodeNullArray, encodeNullBulkString, encodeSimpleError, encodeSimpleString)
import MemoryStore
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import Parser
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout)
import qualified Types as T
import qualified Utilities as U

-- TODO: Eventually will need to turn it into a recursive function that will process all the arguments provided, not just 1 additional one
setCommand :: Socket -> MemoryStore -> BS.ByteString -> BS.ByteString -> Maybe SetExpiry -> IO ()
setCommand socket store key val ex = do
  now <- U.nowNs
  setMemoryDataKey store key $ handleExpiry ex now
  send socket $ encodeSimpleString "OK"
  where
    handleExpiry Nothing timeRef = MemoryStoreEntry (MSStringVal val) Nothing
    handleExpiry (Just (EX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (T.ExpireDuration (fromIntegral $ ex * 1_000_000), T.ExpireReference timeRef)
    handleExpiry (Just (PX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (T.ExpireDuration (fromIntegral ex), T.ExpireReference timeRef)

getCommand :: Socket -> MemoryStore -> BS.ByteString -> IO ()
getCommand socket store key = do
  val <- getMemoryDataVal store key
  case val of
    Nothing -> send socket encodeNullBulkString
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> send socket $ encodeBulkString v
    Just (MemoryStoreEntry (MSStringVal v) (Just (T.ExpireDuration exDur, T.ExpireReference exRef))) -> do
      hasPassed <- U.hasElapsedSince exDur exRef
      if hasPassed
        then do
          delMemoryDataKey store key
          send socket encodeNullBulkString
        else send socket $ encodeBulkString v

pushCommand :: Socket -> MemoryStore -> BS.ByteString -> [BS.ByteString] -> PushCommand -> IO ()
pushCommand socket store key values pushType = do
  val <- getMemoryDataVal store key
  let valuesCount = length values
  case val of
    Nothing -> do
      setMemoryDataKey store key (MemoryStoreEntry (MSListVal $ newItems pushType) Nothing)
      send socket $ encodeInteger valuesCount
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      setMemoryDataKey store key (MemoryStoreEntry (MSListVal $ newList pushType vs) Nothing)
      send socket $ encodeInteger (length vs + valuesCount)
  where
    newItems RightPushCmd = values
    newItems LeftPushCmd = reverse values
    newList RightPushCmd oldList = oldList ++ values
    newList LeftPushCmd oldList = reverse values ++ oldList

lrangeCommand :: Socket -> MemoryStore -> BS.ByteString -> Int -> Int -> IO ()
lrangeCommand socket store key start stop = do
  val <- getMemoryDataVal store key
  case val of
    Nothing -> send socket $ encodeArray True []
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      send socket $ encodeArray True $ go vs (normStart start) (normStop stop)
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

llenCommand :: Socket -> MemoryStore -> BS.ByteString -> IO ()
llenCommand socket store key = do
  val <- getMemoryDataVal store key
  case val of
    Nothing -> send socket $ encodeInteger 0
    Just (MemoryStoreEntry (MSListVal v) Nothing) -> send socket $ encodeInteger $ length v

lpopHelper :: Socket -> MemoryStore -> BS.ByteString -> Int -> IO BS.ByteString
lpopHelper socket store key count = do
  val <- getMemoryDataVal store key
  case val of
    Nothing -> pure encodeNullBulkString
    Just (MemoryStoreEntry (MSListVal v) Nothing) ->
      let normCount = min count $ length v
       in go v socket normCount
      where
        go [] socket _ = pure encodeNullBulkString
        go xs socket c = do
          setMemoryDataKey store key (MemoryStoreEntry (MSListVal (drop c xs)) Nothing)
          pure $ getPopped c v
          where
            getPopped 1 (x : _) = encodeBulkString x
            getPopped popCount xs = encodeArray True (take popCount xs)

lpopCommand :: Socket -> MemoryStore -> BS.ByteString -> Int -> IO ()
lpopCommand socket store key count = do
  resp <- lpopHelper socket store key count
  send socket resp

blpopCommand :: Socket -> MemoryStore -> BS.ByteString -> Double -> Int -> IO ()
blpopCommand socket store key timeout clientID = go 0
  where
    go elapsed
      | elapsed > timeout && timeout > 0 = do
          -- BS8.hPutStrLn stderr $ "ELAPSED:  elapsed: " <> BS8.pack (show elapsed) <> "timeout: " <> BS8.pack (show tout)
          send socket encodeNullArray
      | otherwise = do
          -- BS8.hPutStrLn stderr $ "NOT ELASPED YET elapsed: " <> BS8.pack (show elapsed) <> "timeout: " <> BS8.pack (show timeout)
          val <- getMemoryDataVal store key
          case val of
            Nothing -> do
              addMemoryWaiter store key clientID
              waitAndContinue elapsed
            Just (MemoryStoreEntry (MSListVal []) Nothing) -> do
              addMemoryWaiter store key clientID
              waitAndContinue elapsed
            Just (MemoryStoreEntry (MSListVal (x : xs)) Nothing) -> do
              waiters <- getMemoryWaitersVal store key
              case waiters of
                Nothing -> do
                  resp <- lpopHelper socket store key 1
                  send socket $ encodeArray False [encodeBulkString key, resp]
                Just (BLPopWaiter x : xs) -> do
                  if x == clientID
                    then do
                      resp <- lpopHelper socket store key 1
                      send socket $ encodeArray False [encodeBulkString key, resp]
                      setMemoryWaitersKey store key xs
                    else do
                      addMemoryWaiter store key clientID
                      waitAndContinue elapsed
    waitAndContinue elapsed = do
      threadDelay 1_000
      go $ elapsed + 0.001

typeCommand :: Socket -> MemoryStore -> BS.ByteString -> IO ()
typeCommand socket store key = do
  val <- getMemoryDataVal store key
  case val of
    Nothing -> do
      maybeStream <- getMemoryDataStreams store
      checkIfStream maybeStream
      where
        checkIfStream (Just (MemoryStoreEntry (MSStreams (Streams streams)) _)) = do
          case HM.lookup key streams of
            Just _ -> send socket $ encodeSimpleString "stream"
            Nothing -> send socket $ encodeSimpleString "none"
        checkIfStream Nothing = send socket $ encodeSimpleString "none"
    Just (MemoryStoreEntry (MSStringVal _) _) -> send socket $ encodeSimpleString "string"
    Just (MemoryStoreEntry (MSListVal _) _) -> send socket $ encodeSimpleString "list"

-- xrange ::  (M.Map k a -> Maybe (k, a)) -> ((k -> a -> Bool) -> M.Map k a) -> M.Map k a

getStream :: MemoryStore -> BS.ByteString ->
              (M.Map EntryId RedisStreamValues -> Maybe (EntryId, RedisStreamValues)) ->
              IO (Maybe (EntryId, RedisStreamValues), RedisStream, RedisStreams)
getStream store streamID filter = do
  maybeStreams <- getMemoryDataStreams store
  case maybeStreams of
    Just (MemoryStoreEntry (MSStreams oldStreams) _) -> handleStreams oldStreams
    _                                                -> handleStreams (Streams HM.empty)
  where
    handleStreams :: RedisStreams -> IO (Maybe (EntryId, RedisStreamValues), RedisStream, RedisStreams)
    handleStreams s@(Streams streams) = do
      let os@(Stream oldStream) = fromMaybe (Stream M.empty) (HM.lookup streamID streams)
      pure (filter oldStream, os, s)

xaddCommand :: Socket -> MemoryStore -> BS.ByteString -> EntryId -> RedisStreamValues -> IO ()
xaddCommand socket store streamID (EntryId 0 0) values = send socket (encodeSimpleError "The ID specified in XADD must be greater than 0-0")
xaddCommand socket store streamID EntryGenNew values = U.nowNs >>= \now -> xaddCommand socket store streamID (EntryGenSeq (fromIntegral now)) values
xaddCommand socket store streamID (EntryGenSeq mili) values = do
  (filteredStream, _, streams) <- getStream store streamID (M.lookupMax . M.filterWithKey (\(EntryId m _) _ -> m == mili))
  case filteredStream of
    Nothing -> xaddCommand socket store streamID (EntryId mili (if mili == 0 then 1 else 0)) values
    Just (EntryId m v, _) -> xaddCommand socket store streamID (EntryId mili (v + 1)) values
xaddCommand socket store streamID entryID@(EntryId mili seq) values = do
  (filteredStream, Stream oldStream, streams) <- getStream store streamID (M.lookupMax . M.filterWithKey (\_ _ -> True))
  case filteredStream of
    Nothing -> addNewEntry M.empty streams
    Just (x, _) -> if x >= entryID
                   then send socket $ encodeSimpleError "The ID specified in XADD is equal or smaller than the target stream top item"
                   else addNewEntry oldStream streams
    where
      addNewEntry :: M.Map EntryId RedisStreamValues -> RedisStreams -> IO ()
      addNewEntry oldStream (Streams streams) = do
        let newEntry = values
        let newStream = Stream (M.insert entryID newEntry oldStream)
        let newStreams = HM.insert streamID newStream streams
        setMemoryDataStreams store (MemoryStoreEntry (MSStreams (Streams newStreams)) Nothing)
        send socket $ encodeBulkString (entryIdToBS entryID)

xrangeEndHelper :: MemoryStore -> BS.ByteString -> (EntryId -> RedisStreamValues -> Bool) -> IO (Maybe RangeEntryId)
xrangeEndHelper store key f = do
  (filteredStream, _, streams) <- getStream store key (M.lookupMax . M.filterWithKey f)
  case filteredStream of
    Nothing -> pure Nothing
    Just (EntryId m v, _) -> pure $ Just (RangeEntryId m v)

xrangeHelper :: MemoryStore -> BS.ByteString -> (EntryId -> EntryId -> Bool) -> RangeEntryId -> RangeEntryId -> IO BS.ByteString
xrangeHelper store key rangef (RangeEntryId mili1 mili2) (RangeEntryId seq1 seq2) = do
  (_, Stream oldStream, _) <- getStream store key (const Nothing . M.filterWithKey (\_ _ -> True))
  -- putStrLn $ show ((M.takeWhileAntitone (<= (EntryId 1526985054079 0)) $ M.dropWhileAntitone ((<=) (EntryId 1526985054069 0)) oldStream))
  let allKeysValues = M.toAscList (U.range rangef (EntryId mili1 mili2) (EntryId seq1 seq2) oldStream)
  let resp = parseKeysValues allKeysValues
  pure resp
  where
    parseKeysValues :: [(EntryId, RedisStreamValues)] -> BS.ByteString
    parseKeysValues keysValues = "*" <> (BS8.pack . show . length) keysValues <> "\r\n" <>
                                 foldr (\(id, valuesMap) acc -> "*2\r\n" <> encodeBulkString (entryIdToBS id) <> convertMap valuesMap <> acc) BS.empty keysValues
    convertMap :: RedisStreamValues -> BS.ByteString
    convertMap l = "*" <> (BS8.pack . show . (* 2) . length) l <> "\r\n" <>
                   foldr (\(k, v) acc -> encodeBulkString k <> encodeBulkString v <> acc) BS.empty l

xrangeCommand :: Socket -> MemoryStore -> BS.ByteString -> RangeEntryId -> RangeEntryId -> IO ()
xrangeCommand socket store key mili RangeMinusPlus = do
  res <- xrangeEndHelper store key (\_ _ -> True)
  case res of
    Nothing -> send socket encodeNullArray
    Just seq -> xrangeCommand socket store key mili seq
xrangeCommand socket store key RangeMinusPlus seq = do
  (filteredStream, Stream oldStream, streams) <- getStream store key (M.lookupMin . M.filterWithKey (\_ _ -> True))
  case filteredStream of
    Nothing -> send socket encodeNullArray
    Just (EntryId m v, _) -> xrangeCommand socket store key (RangeEntryId m v) seq
xrangeCommand socket store key (RangeMili mili) seq@(RangeEntryId seq1 seq2) = xrangeCommand socket store key (RangeEntryId mili 0) seq
xrangeCommand socket store key mili@(RangeEntryId _ _) (RangeMili seq) = do
  res <- xrangeEndHelper store key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> send socket $ encodeSimpleError "XRANGE: The end id does not exist"
    Just (RangeEntryId _ newEnd) -> xrangeCommand socket store key mili (RangeEntryId seq newEnd)
xrangeCommand socket store key (RangeMili mili) (RangeMili seq) = do
  res <- xrangeEndHelper store key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> send socket $ encodeSimpleError "XRANGE: The end id does not exist"
    Just (RangeEntryId _ newEnd) -> xrangeCommand socket store key (RangeEntryId mili 0) (RangeEntryId seq newEnd)
xrangeCommand socket store key mili@(RangeEntryId _ _) seq@(RangeEntryId _ _) = do
  resp <- xrangeHelper store key (<) mili seq
  send socket resp

-- (set-face-attribute 'haskell-definition-face nil :weight 'normal :underline t)
-- (set-face-attribute 'haskell-keyword-face nil :weight 'bold)
-- (set-face-attribute 'haskell-constructor-face nil :foreground "#b46069")
-- (set-face-attribute 'flymake-error nil :foreground "red")

xreadEntriesAvailable :: MemoryStore -> [(BS.ByteString, RangeEntryId)] -> IO (Bool, [(BS.ByteString, RangeEntryId)])
xreadEntriesAvailable store keyIds = do
  go keyIds []
  where go :: [(BS.ByteString, RangeEntryId)] -> [(BS.ByteString, RangeEntryId)] -> IO (Bool, [(BS.ByteString, RangeEntryId)])
        go [] newIds = pure (False, newIds)
        go wholelist@(entry@(key, entry_id) : xs) newIds = do
          res <- xrangeEndHelper store key (\_ _ -> True)
          case res of
            Nothing -> case entry_id of
                         RangeDollar -> go xs (newIds ++ [(key, RangeEntryId 0 0)])
                         _           -> go xs (newIds ++ [entry])
            Just r@(RangeEntryId endMili endSeq) -> do
              (_, Stream oldStream, _) <- getStream store key (const Nothing . M.filterWithKey (\_ _ -> True))
              getRange entry_id endMili endSeq oldStream
              where
                getRange :: RangeEntryId -> Word64 -> Word64 -> M.Map EntryId RedisStreamValues -> IO (Bool, [(BS.ByteString, RangeEntryId)])
                getRange (RangeEntryId startMili startSeq) endMili endSeq oldStream = do
                  let allKeysValues = U.range (<=) (EntryId startMili startSeq) (EntryId endMili endSeq) oldStream
                  if null allKeysValues then go xs (newIds ++ [entry]) else pure (True, newIds ++ [entry])
                getRange RangeDollar endMili endSeq oldStream = do
                   let updatedEntry = (key, RangeEntryId endMili endSeq)
                   go xs (newIds ++ [updatedEntry])

-- If there are already entries with IDs greater than the specified ID
xreadCommand :: Socket -> MemoryStore -> [(BS.ByteString, RangeEntryId)] -> Maybe Double -> IO ()
xreadCommand socket store keysIds (Just timeout) = go 0 keysIds
  where
    go elapsed ids = do
      (hasEntries, newIds) <- xreadEntriesAvailable store ids
      if hasEntries
      then xreadCommand socket store newIds Nothing
      else if elapsed > (timeout / 1_000) && timeout > 0
           then send socket encodeNullArray
           else do
             threadDelay 1_000
             go (elapsed + 0.001) newIds

xreadCommand socket store keysIds Nothing = do
  result <- go keysIds BS.empty 0
  send socket $ "*" <> (BS8.pack . show . length) keysIds <> "\r\n" <> result
  where
    go :: [(BS.ByteString, RangeEntryId)] -> BS.ByteString -> Double -> IO BS.ByteString
    go [] acc elapsed = pure acc
    go ((stream_id, entry_id): xs) acc elapsed = do
      res <- xrangeEndHelper store stream_id (\_ _ -> True)
      case res of
        Nothing -> pure encodeNullArray
        Just seq -> do
          streamResp <- xrangeHelper store stream_id (<=) entry_id seq
          go xs (acc <> "*2\r\n" <> encodeBulkString stream_id <> streamResp) elapsed

incrCommand :: Socket -> MemoryStore -> BS.ByteString -> IO ()
incrCommand socket store key = do
  val <- getMemoryDataVal store key
  case val of
    Nothing   -> undefined -- TODO: will be added as one of the next stages
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> case U.bsToInt v of
                                                         Nothing -> undefined
                                                         Just i -> do
                                                           setMemoryDataKey store key (MemoryStoreEntry (MSStringVal $ (BS8.pack . show) (i+1)) Nothing)
                                                           send socket $ encodeInteger (i+1)

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  hSetBuffering stderr NoBuffering

  -- You can use print statements as follows for debugging, they'll be visible when running tests.
  -- hPutStrLn stderr "Logs from your program will appear here"

  store <- newMemoryStore
  nextID <- newTVarIO (0 :: Int)

  let port = "6379"
  putStrLn $ "Redis server listening on port " ++ port
  serve HostAny port $ \(socket, address) -> do
    putStrLn $ "successfully connected client: " ++ show address

    clientID <- atomically $ do
      i <- readTVar nextID
      writeTVar nextID (i + 1)
      pure i

    let loop = do
          received <- recv socket 4096
          case received of
            Nothing -> pure ()
            Just raw_command -> case runParse raw_command of
              Right Ping -> send socket (encodeSimpleString "PONG") >> loop
              Right (Echo str) -> send socket (encodeBulkString str) >> loop
              Right (Set key val args) -> setCommand socket store key val args >> loop
              Right (Get key) -> getCommand socket store key >> loop
              Right (RPush key values) -> pushCommand socket store key values RightPushCmd >> loop
              Right (LPush key values) -> pushCommand socket store key values LeftPushCmd >> loop
              Right (LRange key start stop) -> lrangeCommand socket store key start stop >> loop
              Right (LLen key) -> llenCommand socket store key >> loop
              Right (LPop key count) -> lpopCommand socket store key count >> loop
              Right (BLPop key timeout) -> blpopCommand socket store key timeout clientID >> loop
              Right (Type key) -> typeCommand socket store key >> loop
              Right (XAdd streamID entryID values) -> xaddCommand socket store streamID entryID values >> loop
              Right (XRange key start end) -> xrangeCommand socket store key start end >> loop
              Right (XRead keysIds timeout) -> xreadCommand socket store keysIds timeout >> loop
              Right (Incr key) -> incrCommand socket store key >> loop
              Left e -> hPutStrLn stderr (prettifyErrors e) >> loop

    loop
    closeSock socket
