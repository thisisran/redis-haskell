{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import System.Environment (getArgs)
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout)

import Control.Concurrent.Async (async)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Control.Monad.IO.Class (liftIO)
import Control.Monad (unless, void)

import Data.Word (Word64)
import Data.Maybe (fromMaybe, isNothing, isJust)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve, connect)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.HashMap.Strict as HM
import qualified Data.IntSet as IS
import qualified Data.Map.Strict as M

import Encode
import MemoryStore
import qualified Utilities as U

import Types
import RedisParser
import CliParser

-- TODO: Eventually will need to turn it into a recursive function that will process all the arguments provided, not just 1 additional one
setCommand :: BS.ByteString -> BS.ByteString -> Maybe SetExpiry -> ClientApp BS.ByteString
setCommand key val ex = do
  now <- liftIO U.nowNs
  setDataEntry key $ handleExpiry ex now
  pure $ encodeSimpleString "OK"
  where
    handleExpiry Nothing timeRef = MemoryStoreEntry (MSStringVal val) Nothing
    handleExpiry (Just (EX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (ExpireDuration (fromIntegral $ ex * 1_000_000), ExpireReference timeRef)
    handleExpiry (Just (PX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (ExpireDuration (fromIntegral ex), ExpireReference timeRef)

getCommand :: BS.ByteString -> ClientApp BS.ByteString
getCommand key = do
  tv <- getDataEntry key
  case tv of
    Nothing -> pure encodeNullBulkString
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> pure $ encodeBulkString v
    Just (MemoryStoreEntry (MSStringVal v) (Just (ExpireDuration exDur, ExpireReference exRef))) -> do
      hasPassed <- liftIO $ U.hasElapsedSince exDur exRef
      if hasPassed
        then do
          delDataEntry key
          pure encodeNullBulkString
        else pure $ encodeBulkString v

pushCommand :: BS.ByteString -> [BS.ByteString] -> PushCommand -> ClientApp BS.ByteString
pushCommand key values pushType = do
  val <- getDataEntry key
  let valuesCount = length values
  case val of
    Nothing -> do
      setDataEntry key (MemoryStoreEntry (MSListVal $ newItems pushType) Nothing)
      pure $ encodeInteger valuesCount
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      setDataEntry key (MemoryStoreEntry (MSListVal $ newList pushType vs) Nothing)
      pure $ encodeInteger (length vs + valuesCount)
  where
    newItems RightPushCmd = values
    newItems LeftPushCmd = reverse values
    newList RightPushCmd oldList = oldList ++ values
    newList LeftPushCmd oldList = reverse values ++ oldList

lrangeCommand :: BS.ByteString -> Int -> Int -> ClientApp BS.ByteString
lrangeCommand key start stop = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ encodeArray True []
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      pure $ encodeArray True $ go vs (normStart start) (normStop stop)
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

llenCommand :: BS.ByteString -> ClientApp BS.ByteString
llenCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> pure $ encodeInteger 0
    Just (MemoryStoreEntry (MSListVal v) Nothing) -> pure $ encodeInteger $ length v

lpopHelper :: BS.ByteString -> Int -> ClientApp BS.ByteString
lpopHelper key count = do
  val <- getDataEntry key
  socket <- getSocket
  case val of
    Nothing -> pure encodeNullBulkString
    Just (MemoryStoreEntry (MSListVal v) Nothing) ->
      let normCount = min count $ length v
      in go v socket normCount
      where
        go [] socket _ = pure encodeNullBulkString
        go xs socket c = do
          setDataEntry key (MemoryStoreEntry (MSListVal (drop c xs)) Nothing)
          pure $ getPopped c v
          where
            getPopped 1 (x : _) = encodeBulkString x
            getPopped popCount xs = encodeArray True (take popCount xs)

lpopCommand :: BS.ByteString -> Int -> ClientApp BS.ByteString
lpopCommand = lpopHelper

blpopCommand :: BS.ByteString -> Double -> Int -> ClientApp BS.ByteString
blpopCommand key timeout clientID = go 0
  where
    go elapsed
      | elapsed > timeout && timeout > 0 = do
          pure encodeNullArray
      | otherwise = do
          val <- getDataEntry key
          case val of
            Nothing -> do
              addWaiterOnce key clientID
              waitAndContinue elapsed
            Just (MemoryStoreEntry (MSListVal []) Nothing) -> do
              addWaiterOnce key clientID
              waitAndContinue elapsed
            Just (MemoryStoreEntry (MSListVal (x : xs)) Nothing) -> do
              waiters <- getWaiterEntry key
              case waiters of
                Nothing -> do
                  resp <- lpopHelper key 1
                  pure $ encodeArray False [encodeBulkString key, resp]
                Just waitersList -> do
                  if IS.member clientID waitersList
                    then do
                      resp <- lpopHelper key 1
                      delWaiterEntry key
                      pure $ encodeArray False [encodeBulkString key, resp]
                    else do
                      addWaiterOnce key clientID
                      waitAndContinue elapsed
    waitAndContinue elapsed = do
      liftIO $ threadDelay 1_000
      go $ elapsed + 0.001

typeCommand :: BS.ByteString -> ClientApp BS.ByteString
typeCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      (Streams streams) <- getStreams
      case HM.lookup key streams of
        Just _ -> pure $ encodeSimpleString "stream"
        Nothing -> pure $ encodeSimpleString "none"
    Just (MemoryStoreEntry (MSStringVal _) _) -> pure $ encodeSimpleString "string"
    Just (MemoryStoreEntry (MSListVal _) _) -> pure $ encodeSimpleString "list"

xaddCommand :: BS.ByteString -> EntryId -> RedisStreamValues -> ClientApp BS.ByteString
xaddCommand streamID (EntryId 0 0) values = pure $ encodeSimpleError "The ID specified in XADD must be greater than 0-0"
xaddCommand streamID EntryGenNew values = liftIO U.nowNs >>= \now -> xaddCommand streamID (EntryGenSeq (fromIntegral now)) values
xaddCommand streamID (EntryGenSeq mili) values = do
  (filteredStream, _, _) <- getStream streamID (M.lookupMax . M.filterWithKey (\(EntryId m _) _ -> m == mili))
  case filteredStream of
    Nothing -> xaddCommand streamID (EntryId mili (if mili == 0 then 1 else 0)) values
    Just (EntryId m v, _) -> xaddCommand streamID (EntryId mili (v + 1)) values
xaddCommand streamID entryID@(EntryId mili seq) values = do
  (filteredStream, Stream oldStream, streams) <- getStream streamID (M.lookupMax . M.filterWithKey (\_ _ -> True))
  case filteredStream of
    Nothing -> addNewEntry M.empty streams
    Just (x, _) ->
      if x >= entryID
        then pure $ encodeSimpleError "The ID specified in XADD is equal or smaller than the target stream top item"
        else addNewEntry oldStream streams
  where
    addNewEntry :: M.Map EntryId RedisStreamValues -> RedisStreams -> ClientApp BS.ByteString
    addNewEntry oldStream (Streams streams) = do
      let newEntry = values
      let newStream = Stream (M.insert entryID newEntry oldStream)
      let newStreams = HM.insert streamID newStream streams
      setStreams (MemoryStoreEntry (MSStreams (Streams newStreams)) Nothing)
      pure $ encodeBulkString (U.entryIdToBS entryID)

xrangeEndHelper :: BS.ByteString -> (EntryId -> RedisStreamValues -> Bool) -> ClientApp (Maybe RangeEntryId)
xrangeEndHelper key f = do
  (filteredStream, _, streams) <- getStream key (M.lookupMax . M.filterWithKey f)
  case filteredStream of
    Nothing -> pure Nothing
    Just (EntryId m v, _) -> pure $ Just (RangeEntryId m v)

xrangeHelper :: BS.ByteString -> (EntryId -> EntryId -> Bool) -> RangeEntryId -> RangeEntryId -> ClientApp BS.ByteString
xrangeHelper key rangef (RangeEntryId mili1 mili2) (RangeEntryId seq1 seq2) = do
  (_, Stream oldStream, _) <- getStream key (const Nothing . M.filterWithKey (\_ _ -> True))
  -- putStrLn $ show ((M.takeWhileAntitone (<= (EntryId 1526985054079 0)) $ M.dropWhileAntitone ((<=) (EntryId 1526985054069 0)) oldStream))
  let allKeysValues = M.toAscList (U.range rangef (EntryId mili1 mili2) (EntryId seq1 seq2) oldStream)
  let resp = parseKeysValues allKeysValues
  pure resp
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

xrangeCommand :: BS.ByteString -> RangeEntryId -> RangeEntryId -> ClientApp BS.ByteString
xrangeCommand key mili RangeMinusPlus = do
  res <- xrangeEndHelper key (\_ _ -> True)
  case res of
    Nothing -> pure encodeNullArray
    Just seq -> xrangeCommand key mili seq
xrangeCommand key RangeMinusPlus seq = do
  (filteredStream, Stream oldStream, streams) <- getStream key (M.lookupMin . M.filterWithKey (\_ _ -> True))
  case filteredStream of
    Nothing -> pure encodeNullArray
    Just (EntryId m v, _) -> xrangeCommand key (RangeEntryId m v) seq
xrangeCommand key (RangeMili mili) seq@(RangeEntryId seq1 seq2) = xrangeCommand key (RangeEntryId mili 0) seq
xrangeCommand key mili@(RangeEntryId _ _) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ encodeSimpleError "XRANGE: The end id does not exist"
    Just (RangeEntryId _ newEnd) -> xrangeCommand key mili (RangeEntryId seq newEnd)
xrangeCommand key (RangeMili mili) (RangeMili seq) = do
  res <- xrangeEndHelper key (\(EntryId m _) _ -> m == seq)
  case res of
    Nothing -> pure $ encodeSimpleError "XRANGE: The end id does not exist"
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

xreadCommand :: [(BS.ByteString, RangeEntryId)] -> Maybe Double -> ClientApp BS.ByteString
xreadCommand keysIds (Just timeout) = go 0 keysIds
  where
    go elapsed ids = do
      (hasEntries, newIds) <- xreadEntriesAvailable ids
      if hasEntries
        then xreadCommand newIds Nothing
        else
          if elapsed > (timeout / 1_000) && timeout > 0
            then pure encodeNullArray
            else do
              liftIO $ threadDelay 1_000
              go (elapsed + 0.001) newIds
xreadCommand keysIds Nothing = do
  result <- go keysIds BS.empty 0
  pure $ "*" <> (BS8.pack . show . length) keysIds <> "\r\n" <> result
  where
    go :: [(BS.ByteString, RangeEntryId)] -> BS.ByteString -> Double -> ClientApp BS.ByteString
    go [] acc elapsed = pure acc
    go ((stream_id, entry_id) : xs) acc elapsed = do
      res <- xrangeEndHelper stream_id (\_ _ -> True)
      case res of
        Nothing -> pure encodeNullArray
        Just seq -> do
          streamResp <- xrangeHelper stream_id (<=) entry_id seq
          go xs (acc <> "*2\r\n" <> encodeBulkString stream_id <> streamResp) elapsed

incrCommand :: BS.ByteString -> ClientApp BS.ByteString
incrCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      setDataEntry key (MemoryStoreEntry (MSStringVal "1") Nothing)
      pure $ encodeInteger 1
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> case U.bsToInt v of
      Nothing -> pure $ encodeSimpleError "value is not an integer or out of range"
      Just i -> do
        setDataEntry key (MemoryStoreEntry (MSStringVal $ (BS8.pack . show) (i + 1)) Nothing)
        pure $ encodeInteger (i + 1)

execCommand :: ClientApp BS.ByteString
execCommand = do
  multi <- getMulti
  updateMulti False
  if multi
  then do
    ml <- getMultiList
    if null ml
    then pure $ encodeArray True []
    else do
      ml <- getMultiList
      res <- go ml []
      pure $ encodeArray False res
  else pure $ encodeSimpleError "EXEC without MULTI"
  where go :: [ClientApp BS.ByteString] -> [BS.ByteString] -> ClientApp [BS.ByteString]
        go [] acc = pure acc
        go (x:xs) acc = do
           resp <- x
           go xs (acc ++ [resp])

discardCommand :: ClientApp BS.ByteString
discardCommand = do
  multi <- getMulti
  if multi
  then do
    updateMulti False
    resetMultiCommands
    pure $ encodeSimpleString "OK"
  else pure $ encodeSimpleError "DISCARD without MULTI"

handleMultiCmd :: ClientApp BS.ByteString -> ClientApp BS.ByteString
handleMultiCmd op = do
  multi <- getMulti
  if multi
  then do
    addMultiCommand op
    pure $ encodeSimpleString "QUEUED"
  else op

infoCommand :: InfoRequest -> ClientApp BS.ByteString
infoCommand Replication = do
  role <- getRole
  case role of
    Master repID repOffset -> pure $ encodeBulkString $ "# Replication\nrole:master\nmaster_replid:" <> BS8.pack repID <> "\nmaster_repl_offset:" <> (BS8.pack . show) repOffset
    Slave roHost roPort -> pure $ encodeBulkString "# Replication\nrole:slave"

replConfCommand :: ReplConfOptions -> ClientApp BS.ByteString
replConfCommand (ListeningPort port) = do
  pure $ encodeSimpleString "OK"
replConfCommand (Capa capa) = do
  pure $ encodeSimpleString "OK"

handleConnection :: ClientApp ()
handleConnection = go
  where
    go :: ClientApp ()
    go = do
      sock <- getSocket
      clientID <- getClientID
      mb <- liftIO $ recv sock 4096
      case mb of
        Nothing -> pure ()
        Just buf -> do
          resp <- case runParse buf of
            Right Ping -> handleMultiCmd $ pure (encodeSimpleString "PONG")
            Right (Echo str) -> handleMultiCmd $ pure $ encodeBulkString str
            Right (Set key val args) -> handleMultiCmd $ setCommand key val args
            Right (Get key) -> handleMultiCmd $ getCommand key
            Right (RPush key values) -> handleMultiCmd $ pushCommand key values RightPushCmd
            Right (LPush key values) -> handleMultiCmd $ pushCommand key values LeftPushCmd
            Right (LRange key start stop) -> handleMultiCmd $ lrangeCommand key start stop
            Right (LLen key) -> handleMultiCmd $ llenCommand key
            Right (LPop key count) -> handleMultiCmd $ lpopCommand key count
            Right (BLPop key timeout) -> handleMultiCmd $ blpopCommand key timeout clientID
            Right (Type key) -> handleMultiCmd $ typeCommand key
            Right (XAdd streamID entryID values) -> handleMultiCmd $ xaddCommand streamID entryID values
            Right (XRange key start end) -> handleMultiCmd $ xrangeCommand key start end
            Right (XRead keysIds timeout) -> handleMultiCmd $ xreadCommand keysIds timeout
            Right (Incr key) -> handleMultiCmd $ incrCommand key
            Right Multi -> updateMulti True >> pure (encodeSimpleString "OK")
            Right Exec -> execCommand
            Right Discard -> discardCommand
            Right (Info infoRequest) -> handleMultiCmd $ infoCommand infoRequest
            Right (ReplConf replOptions) -> replConfCommand replOptions
            Left e -> pure $ U.renderParseError e
          liftIO $ send sock resp
          go

recvExactOneReply :: Network.Simple.TCP.Socket -> IO BS.ByteString
recvExactOneReply sock = go mempty
  where
    go acc =
      recv sock 4096 >>= \case
        Nothing -> pure acc
        Just chunk
          | BS.null chunk -> go acc
          | otherwise     -> pure (acc <> chunk)

runReplica :: App ()
runReplica = do
  clientPort <- getPort
  repl <- getReplication
  case repl of
    Slave host port -> do
      liftIO $ putStrLn ("Trying to connect to " <> host <> " " <> port)
      _ <- liftIO $ async $ connect host port $ \(_sock, _addr) -> do
        send _sock $ encodeArray True ["PING"]
        pongResp <- liftIO $ recvExactOneReply _sock
        case runSimpleStringParse pongResp of
          Right "PONG" -> do
            liftIO $ putStrLn "successfully received an OK!"
            liftIO $ send _sock $ encodeArray True ["REPLCONF", "listening-port", BS8.pack clientPort]
            replResp1 <- liftIO $ recvExactOneReply _sock
            case runSimpleStringParse replResp1 of
              Right "OK" -> do
                liftIO $ putStrLn "Got confirmation form REPLCONF listening port"
                liftIO $ send _sock $ encodeArray True ["REPLCONF", "capa", "psync2"]
                replResp2 <- liftIO $ recvExactOneReply _sock
                case runSimpleStringParse replResp1 of
                  Right "OK" -> do
                   liftIO $ putStrLn "Got confirmation form REPLCONF capability"
                   liftIO $ send _sock $ encodeArray True ["PSYNC", "?", "-1"]
      pure mempty
    Master _ _ -> pure mempty

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

  repID <- U.randomAlphaNum40BS
  let sharedCfg = case cliReplication cfgCli of
              WantMaster -> SharedConfig port (Master (BS8.unpack repID) 0)
              WantSlave wantHost wantPort -> SharedConfig port (Slave wantHost wantPort)

  let sharedEnv = SharedEnv store sharedCfg

  case sharedCfg of
    SharedConfig _ (Master _ _) -> pure ()
    SharedConfig _ (Slave _ _) -> runApp sharedEnv runReplica

  putStrLn $ "Redis server listening on port " ++ port
  serve HostAny port $ \(socket, address) -> do
    putStrLn $ "successfully connected client: " ++ show address

    clientID <- atomically $ do
      i <- readTVar nextID
      writeTVar nextID (i + 1)
      pure i

    let cs = ClientState {multi = False, multiList = [] }

    let clientCfg = ClientConfig clientID socket sharedCfg
    let env = ClientEnv sharedEnv clientCfg

    runClientApp env cs handleConnection
    closeSock socket
