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
setCommand :: BS.ByteString -> BS.ByteString -> Maybe SetExpiry -> ClientApp Response
setCommand key val ex = do
  now <- liftIO U.nowNs
  setDataEntry key $ handleExpiry ex now
  pure $ Response (encodeSimpleString "OK") emptyResponse
  where
    handleExpiry Nothing timeRef = MemoryStoreEntry (MSStringVal val) Nothing
    handleExpiry (Just (EX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (ExpireDuration (fromIntegral $ ex * 1_000_000), ExpireReference timeRef)
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

pushCommand :: BS.ByteString -> [BS.ByteString] -> PushCommand -> ClientApp Response
pushCommand key values pushType = do
  val <- getDataEntry key
  let valuesCount = length values
  case val of
    Nothing -> do
      setDataEntry key (MemoryStoreEntry (MSListVal $ newItems pushType) Nothing)
      pure $ Response (encodeInteger valuesCount) emptyResponse
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      setDataEntry key (MemoryStoreEntry (MSListVal $ newList pushType vs) Nothing)
      pure $ Response (encodeInteger (length vs + valuesCount)) emptyResponse
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

lpopHelper :: BS.ByteString -> Int -> ClientApp Response
lpopHelper key count = do
  val <- getDataEntry key
  socket <- getSocket
  case val of
    Nothing -> pure $ Response encodeNullBulkString emptyResponse
    Just (MemoryStoreEntry (MSListVal v) Nothing) ->
      let normCount = min count $ length v
      in go v socket normCount
      where
        go [] socket _ = pure $ Response encodeNullBulkString emptyResponse
        go xs socket c = do
          setDataEntry key (MemoryStoreEntry (MSListVal (drop c xs)) Nothing)
          pure $ Response (getPopped c v) emptyResponse
          where
            getPopped 1 (x : _) = encodeBulkString x
            getPopped popCount xs = encodeArray True (take popCount xs)

lpopCommand :: BS.ByteString -> Int -> ClientApp Response
lpopCommand = lpopHelper

blpopCommand :: BS.ByteString -> Double -> Int -> ClientApp Response
blpopCommand key timeout clientID = go 0
  where
    go elapsed
      | elapsed > timeout && timeout > 0 = do
          pure $ Response encodeNullArray emptyResponse
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
                  (Response resp _) <- lpopHelper key 1
                  pure $ Response (encodeArray False [encodeBulkString key, resp]) emptyResponse
                Just waitersList -> do
                  if IS.member clientID waitersList
                    then do
                      (Response resp _) <- lpopHelper key 1
                      delWaiterEntry key
                      pure $ Response (encodeArray False [encodeBulkString key, resp]) emptyResponse
                    else do
                      addWaiterOnce key clientID
                      waitAndContinue elapsed
    waitAndContinue elapsed = do
      liftIO $ threadDelay 1_000
      go $ elapsed + 0.001

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

xaddCommand :: BS.ByteString -> EntryId -> RedisStreamValues -> ClientApp Response
xaddCommand streamID (EntryId 0 0) values = pure $ Response (encodeSimpleError "The ID specified in XADD must be greater than 0-0") emptyResponse
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
        then pure $ Response (encodeSimpleError "The ID specified in XADD is equal or smaller than the target stream top item") emptyResponse
        else addNewEntry oldStream streams
  where
    addNewEntry :: M.Map EntryId RedisStreamValues -> RedisStreams -> ClientApp Response
    addNewEntry oldStream (Streams streams) = do
      let newEntry = values
      let newStream = Stream (M.insert entryID newEntry oldStream)
      let newStreams = HM.insert streamID newStream streams
      setStreams (MemoryStoreEntry (MSStreams (Streams newStreams)) Nothing)
      pure $ Response (encodeBulkString (U.entryIdToBS entryID)) emptyResponse

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

incrCommand :: BS.ByteString -> ClientApp Response
incrCommand key = do
  val <- getDataEntry key
  case val of
    Nothing -> do
      setDataEntry key (MemoryStoreEntry (MSStringVal "1") Nothing)
      pure $ Response (encodeInteger 1) emptyResponse
    Just (MemoryStoreEntry (MSStringVal v) Nothing) -> case U.bsToInt v of
      Nothing -> pure $ Response (encodeSimpleError "value is not an integer or out of range") emptyResponse
      Just i -> do
        setDataEntry key (MemoryStoreEntry (MSStringVal $ (BS8.pack . show) (i + 1)) Nothing)
        pure $ Response (encodeInteger $ i + 1) emptyResponse

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

emptyRdbFile :: BS.ByteString
emptyRdbFile = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

sendSnapshot :: ClientApp ()
sendSnapshot = do
  socket <- getSocket
  case U.decodeRdbBase64 emptyRdbFile of
    Right x -> liftIO $ send socket $ encodeRdbFile x

psyncCommand :: PSyncRequest -> ClientApp Response
psyncCommand PSyncUnknown = do
  repl <- getClientReplication
  case repl of
    Master repID _ -> pure $ Response (encodeSimpleString $ "FULLRESYNC " <> BS8.pack repID <> " 0") sendSnapshot
    _ -> pure $ Response mempty emptyResponse

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
          (Response resp nextAction) <- case runParse buf of
            Right Ping -> handleMultiCmd $ pure $ Response (encodeSimpleString "PONG") emptyResponse
            Right (Echo str) -> handleMultiCmd $ pure $ Response (encodeBulkString str) emptyResponse
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
            Right Multi -> do
              updateMulti True
              pure $ Response (encodeSimpleString "OK") emptyResponse
            Right Exec -> execCommand
            Right Discard -> discardCommand
            Right (Info infoRequest) -> handleMultiCmd $ infoCommand infoRequest
            Right (ReplConf replOptions) -> replConfCommand replOptions
            Right (Psync req) -> psyncCommand req
            Left e -> pure $ Response (U.renderParseError e) emptyResponse
          liftIO $ send sock resp
          nextAction
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
            -- liftIO $ putStrLn "successfully received an OK!"
            liftIO $ send _sock $ encodeArray True ["REPLCONF", "listening-port", BS8.pack clientPort]
            replResp1 <- liftIO $ recvExactOneReply _sock
            case runSimpleStringParse replResp1 of
              Right "OK" -> do
                -- liftIO $ putStrLn "Got confirmation form REPLCONF listening port"
                liftIO $ send _sock $ encodeArray True ["REPLCONF", "capa", "psync2"]
                replResp2 <- liftIO $ recvExactOneReply _sock
                case runSimpleStringParse replResp1 of
                  Right "OK" -> do
                   -- liftIO $ putStrLn "Got confirmation form REPLCONF capability"
                   liftIO $ send _sock $ encodeArray True ["PSYNC", "?", "-1"]
                   -- TODO: parse replPsync to get the replica ID and the offset
                   replPsync <- liftIO $ recvExactOneReply _sock
                   -- TODO: parse Rdb file response and update the DB accordingly
                   pure $ recvExactOneReply _sock
                   
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
