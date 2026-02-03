{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import Data.Maybe (fromMaybe)
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Encode (encodeArray, encodeBulkString, encodeInteger, encodeNullArray, encodeNullBulkString, encodeSimpleString)
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
  where handleExpiry Nothing timeRef = MemoryStoreEntry (MSStringVal val) Nothing
        handleExpiry (Just (EX ex)) timeRef = MemoryStoreEntry (MSStringVal val) $ Just (T.ExpireDuration (fromIntegral $ ex * 1_000), T.ExpireReference timeRef)
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
          setMemoryDataKey store key (MemoryStoreEntry (MSListVal (drop c xs))  Nothing)
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
      maybeStream <- getMemoryDataVal store "streams"  
      checkIfStream maybeStream 
      where
        checkIfStream (Just (MemoryStoreEntry (MSStreams (Streams streams)) _)) = do
          case HM.lookup key streams of
            Just _ -> send socket $ encodeSimpleString "stream"
            Nothing -> send socket $ encodeSimpleString "none"
        checkIfStream Nothing = send socket $ encodeSimpleString "none"
    Just (MemoryStoreEntry (MSStringVal _) _) -> send socket $ encodeSimpleString "string"
    Just (MemoryStoreEntry (MSListVal _) _) -> send socket $ encodeSimpleString "list"

xaddCommand :: Socket -> MemoryStore -> BS.ByteString -> EntryId -> [(BS.ByteString, BS.ByteString)] -> IO ()
xaddCommand socket store streamID entryID values = do
  mayStreams <- getMemoryDataVal store "streams"
  case mayStreams of
    Just (MemoryStoreEntry (MSStreams oldStreams) _) -> handleStreams oldStreams
    _                                                -> handleStreams (Streams HM.empty)
    where
      handleStreams :: Streams BS.ByteString (M.Map BS.ByteString BS.ByteString) -> IO ()
      handleStreams (Streams streams) = do
        let (Stream oldStream) = fromMaybe (Stream M.empty) (HM.lookup streamID streams)
        let newEntry = insertValues values M.empty
        let newStream = Stream (M.insert entryID newEntry oldStream)
        let newStreams = HM.insert streamID newStream streams
        setMemoryDataKey store "streams" (MemoryStoreEntry (MSStreams (Streams newStreams)) Nothing)
        send socket $ encodeBulkString (entryIdToBS entryID)
        where
          insertValues ((key, value) : xs) m = insertValues xs (M.insert key value m)
          insertValues [] m = m

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
              Left e -> hPutStrLn stderr (prettifyErrors e) >> loop

    loop
    closeSock socket
