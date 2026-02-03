{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Main (main) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import Encode (encodeArray, encodeBulkString, encodeInteger, encodeNullArray, encodeNullBulkString, encodeSimpleString)
import MemoryStore
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import Parser (parseCommand)
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout)
import qualified Types as T
import qualified Utilities as U

---------------------------------------------------------------------------------------------------------------

handlePushCommand :: Bool -> T.CommandArguments -> MemoryStore -> Socket -> IO ()
handlePushCommand isRPush (key : xs) store sock = do
  val <- getMemoryDataVal store key
  case val of
    Nothing -> do
      setMemoryDataKey store key (MemoryStoreEntry (MSListVal newItems) Nothing)
      send sock $ encodeInteger (length xs)
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      setMemoryDataKey store key (MemoryStoreEntry (MSListVal $ newList vs) Nothing)
      send sock $ encodeInteger (length vs + length xs)
  where
    newItems = if isRPush then xs else reverse xs
    newList oldList = if isRPush then oldList ++ newItems else newItems ++ oldList
handlePushCommand _ _ _ _ = pure () -- TODO: this would report a command argument error

handleRPushCommand :: T.CommandArguments -> MemoryStore -> Socket -> IO ()
handleRPushCommand = handlePushCommand True

handleLPushCommand :: T.CommandArguments -> MemoryStore -> Socket -> IO ()
handleLPushCommand = handlePushCommand False

lpopHelper :: T.DataKey -> Maybe Int -> MemoryStore -> Socket -> IO T.BLPopResponse
lpopHelper key (Just count) store sock = do
  val <- getMemoryDataVal store key
  case val of
    Nothing -> pure encodeNullBulkString
    Just (MemoryStoreEntry (MSListVal v) Nothing) ->
      let normCount = min count $ length v
       in go v sock normCount
      where
        go [] socket _ = pure encodeNullBulkString
        go xs socket c = do
          setMemoryDataKey store key (MemoryStoreEntry (MSListVal (drop c xs)) Nothing)
          pure $ getPopped c v
          where
            getPopped 1 (x : _) = encodeBulkString x
            getPopped popCount xs = encodeArray True (take popCount xs)
lpopHelper key Nothing store sock = lpopHelper key (Just 1) store sock

handleLPopCommand :: T.CommandArguments -> MemoryStore -> Socket -> IO ()
handleLPopCommand (key : count : _) store sock = do
  resp <- lpopHelper key (U.bsToInt count) store sock
  send sock resp
handleLPopCommand (key : _) store sock = do
  resp <- lpopHelper key (Just 1) store sock
  send sock resp

handleBLPop :: T.CommandArguments -> MemoryStore -> Socket -> T.ClientID -> IO ()
handleBLPop (key : timeout : _) store sock cid = case U.bsToDouble timeout of
  Nothing -> pure () -- send error
  Just tout -> go 0
    where
      go elapsed
        | elapsed > tout && tout > 0 = do
            -- BS8.hPutStrLn stderr $ "ELAPSED:  elapsed: " <> BS8.pack (show elapsed) <> "timeout: " <> BS8.pack (show tout)
            send sock encodeNullArray
        | otherwise = do
            -- BS8.hPutStrLn stderr $ "NOT ELASPED YET elapsed: " <> BS8.pack (show elapsed) <> "timeout: " <> BS8.pack (show timeout)
            val <- getMemoryDataVal store key
            case val of
              Nothing -> do
                addMemoryWaiter store key cid
                waitAndContinue elapsed
              Just (MemoryStoreEntry (MSListVal []) Nothing) -> do
                addMemoryWaiter store key cid
                waitAndContinue elapsed
              Just (MemoryStoreEntry (MSListVal (x : xs)) Nothing) -> do
                waiters <- getMemoryWaitersVal store key
                case waiters of
                  Nothing -> do
                    resp <- lpopHelper key (Just 1) store sock
                    send sock $ encodeArray False [encodeBulkString key, resp]
                  Just (BLPopWaiter x : xs) -> do
                    if x == cid
                      then do
                        resp <- lpopHelper key (Just 1) store sock
                        send sock $ encodeArray False [encodeBulkString key, resp]
                        setMemoryWaitersKey store key xs
                      else do
                        addMemoryWaiter store key cid
                        waitAndContinue elapsed

      waitAndContinue elapsed = do
        threadDelay 1_000
        go $ elapsed + 0.001

handleLRangeCommand :: T.CommandArguments -> MemoryStore -> Socket -> IO ()
handleLRangeCommand xs store sock = case xs of
    (key : start : stop : _) -> do
      val <- getMemoryDataVal store key
      case val of
        Nothing -> send sock $ encodeArray True []
        Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
          let itemCount = length vs
          case (U.bsToInt start, U.bsToInt stop) of
            (Just s, Just st) -> send sock $ encodeArray True $ go vs (normStart s) (normStop st)
              where
                go :: [BS.ByteString] -> Int -> Int -> [BS.ByteString]
                go xs from to
                  | s >= itemCount || from > to = []
                  | otherwise = (take (to - from + 1) . drop from) xs
                normStart index
                  | index >= 0 = index
                  | -index > itemCount = 0
                  | otherwise = itemCount + index
                normStop index
                  | index >= 0 = (if index >= itemCount then itemCount - 1 else index)
                  | -index > itemCount = 0
                  | otherwise = itemCount + index
            _ -> pure () -- TODO: this would report an error about converting the arg to an int
    _ -> pure () -- TODO: this would report a command argument error for the rpush command

---------------------------------------------------------------------------------------------------------------

handleCommand :: Socket -> MemoryStore -> T.ClientID -> [BS.ByteString] -> IO ()
handleCommand sock store _ [] = pure ()
handleCommand sock store cid (x : xs) = case U.bsToLower x of
  "ping" -> send sock $ encodeSimpleString "PONG"
  "echo" -> case xs of
    (arg : _) -> send sock $ encodeBulkString arg
    _ -> pure () -- TODO: this would report a command argument error
  "set" -> case xs of
    (key : val : xs) -> do
      now <- U.nowNs
      setMemoryDataKey store key $ handleArguments val xs now
      send sock $ encodeSimpleString "OK"
      where
        -- TODO: Eventually will need to turn it into a recursive function that will process all the arguments provided
        -- Not just 1 additional one
        handleArguments :: BS.ByteString -> [BS.ByteString] -> Integer -> MemoryStoreEntry
        handleArguments v (y1 : y2 : _) now = case U.bsToLower y1 of
          "px" ->
            let exp = U.bsToInteger y2
             in case exp of
                  Nothing -> MemoryStoreEntry (MSStringVal v) Nothing -- TODO: this would report a command argument error
                  Just y2Int -> MemoryStoreEntry (MSStringVal v) $ Just (T.ExpireDuration y2Int, T.ExpireReference now)
          "ex" ->
            let exp = U.bsToInteger y2
             in case exp of
                  Nothing -> MemoryStoreEntry (MSStringVal v) Nothing -- TODO: this would report a command argument error
                  Just y2Int -> MemoryStoreEntry (MSStringVal v) $ Just (T.ExpireDuration $ y2Int * 1_000, T.ExpireReference now)
        handleArguments v _ _ = MemoryStoreEntry (MSStringVal v) Nothing
    _ -> pure () -- TODO: this would report a command argument error for the set command
  "get" -> case xs of
    (key : _) -> do
      val <- getMemoryDataVal store key
      case val of
        Nothing -> send sock encodeNullBulkString
        Just (MemoryStoreEntry (MSStringVal v) Nothing) -> send sock $ encodeBulkString v
        Just (MemoryStoreEntry (MSStringVal v) (Just (T.ExpireDuration exDur, T.ExpireReference exRef))) -> do
          hasPassed <- U.hasElapsedSince exDur exRef
          if hasPassed
            then do
              delMemoryDataKey store key
              send sock encodeNullBulkString
            else send sock $ encodeBulkString v
  "rpush" -> handleRPushCommand xs store sock
  "lpush" -> handleLPushCommand xs store sock
  "lrange" -> handleLRangeCommand xs store sock
  "llen" -> case xs of
    (key : _) -> do
      val <- getMemoryDataVal store key
      case val of
        Nothing -> send sock $ encodeInteger 0
        Just (MemoryStoreEntry (MSListVal v) Nothing) -> send sock $ encodeInteger $ length v
    _ -> pure () -- TODO: this would report a command argument error for the llen command
  "lpop" -> handleLPopCommand xs store sock
  "blpop" -> handleBLPop xs store sock cid
  "type" -> case xs of
    (key : _) -> do
      val <- getMemoryDataVal store key
      case val of
        Nothing -> send sock $ encodeSimpleString "none"
        Just v -> send sock $ encodeSimpleString "string"
    _ -> pure () -- TODO: this would report a command argument error for the type command
  "xadd" -> case xs of
    (stream_key : stream_id : xs) -> undefined
    _ -> pure () -- TODO: this would report a command argument error for the xadd command
  _ -> pure () -- TODO: this would report a command error

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

    cid <- atomically $ do
      i <- readTVar nextID
      writeTVar nextID (i + 1)
      pure i

    let loop = do
          received <- recv socket 4096
          case received of
            Nothing -> pure ()
            Just raw_command -> case parseCommand raw_command of
              Right message -> handleCommand socket store cid message >> loop
              Left _ -> loop

    loop
    closeSock socket
