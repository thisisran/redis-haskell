{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Network.Simple.TCP (serve, recv, send, HostPreference(HostAny), closeSock, Socket)
import System.IO (hPutStrLn, hSetBuffering, stdout, stderr, BufferMode(NoBuffering))

import Parser (parseCommand)
import qualified Types as T
import MemoryStore
import Encode (encodeBulkString, encodeNullBulkString, encodeSimpleString, encodeInteger, encodeArray)
import qualified Utilities as U

import qualified Data.ByteString as BS

handlePushCommand :: Bool -> [BS.ByteString] -> MemoryStore -> Socket -> IO ()
handlePushCommand isRPush (key : xs) store sock = do
  val <- getMemoryStoreVal store key
  case val of
    Nothing -> do
      setMemoryStoreKey store key (MemoryStoreEntry (MSListVal newItems) Nothing)
      send sock $ encodeInteger (length xs)
    Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
      setMemoryStoreKey store key (MemoryStoreEntry (MSListVal $ newList vs) Nothing)
      send sock $ encodeInteger (length vs + length xs)
  where newItems = if isRPush then xs else reverse xs
        newList oldList = if isRPush then oldList ++ newItems else newItems ++ oldList
handlePushCommand _ _ _ _ = pure () -- TODO: this would report a command argument error

handleRPushCommand :: [BS.ByteString] -> MemoryStore -> Socket -> IO ()
handleRPushCommand = handlePushCommand True

handleLPushCommand :: [BS.ByteString] -> MemoryStore -> Socket -> IO ()
handleLPushCommand = handlePushCommand False

lpopHelper :: BS.ByteString -> Maybe Int -> MemoryStore -> Socket -> IO ()
lpopHelper key (Just count) store sock = do
  val <- getMemoryStoreVal store key
  case val of
    Nothing -> send sock encodeNullBulkString
    Just (MemoryStoreEntry (MSListVal v) Nothing) ->
      let normCount = min count $ length v
      in go v sock normCount
         where go [] socket _ = send socket encodeNullBulkString
               go xs socket c = do
                 setMemoryStoreKey store key (MemoryStoreEntry (MSListVal (drop c xs)) Nothing)
                 send socket $ getPopped c v
                 where getPopped 1 (x:_) = encodeBulkString x
                       getPopped popCount xs = encodeArray (take popCount xs)
lpopHelper key Nothing store sock = lpopHelper key (Just 1) store sock
                
handleLPopCommand :: [BS.ByteString] -> MemoryStore -> Socket -> IO ()
handleLPopCommand (key : count : _) store sock = lpopHelper key (U.bsToInt count) store sock
handleLPopCommand (key : _) store sock = lpopHelper key (Just 1) store sock 

handleCommand :: Socket -> MemoryStore -> [BS.ByteString] -> IO ()
handleCommand sock store [] = pure ()
handleCommand sock store (x:xs) = case U.bsToLower x of
                                         "ping" -> send sock $ encodeSimpleString "PONG"
                                         "echo" -> case xs of
                                                     (arg : _) -> send sock $ encodeBulkString arg
                                                     _              -> pure () -- TODO: this would report a command argument error
                                         "set"  -> case xs of
                                                     (key : val : xs) -> do
                                                       now <- U.nowNs
                                                       setMemoryStoreKey store key $ handleArguments val xs now
                                                       send sock $ encodeSimpleString "OK"
                                                       where
                                                         -- TODO: Eventually will need to turn it into a recursive function that will process all the arguments provided
                                                         -- Not just 1 additional one
                                                         handleArguments :: BS.ByteString -> [BS.ByteString] -> Integer -> MemoryStoreEntry
                                                         handleArguments v (y1 : y2 : _) now = case U.bsToLower y1 of
                                                           "px" -> let exp = U.bsToInteger y2
                                                                   in
                                                                     case exp of
                                                                       Nothing -> MemoryStoreEntry (MSStringVal v) Nothing -- TODO: this would report a command argument error
                                                                       Just y2Int -> MemoryStoreEntry (MSStringVal v) $ Just (T.ExpireDuration y2Int, T.ExpireReference now)
                                                           "ex" -> let exp = U.bsToInteger y2
                                                                   in
                                                                     case exp of
                                                                       Nothing -> MemoryStoreEntry (MSStringVal v) Nothing -- TODO: this would report a command argument error
                                                                       Just y2Int -> MemoryStoreEntry (MSStringVal v) $ Just (T.ExpireDuration $ y2Int * 1000, T.ExpireReference now)
                                                         handleArguments v _ _ = MemoryStoreEntry (MSStringVal v) Nothing
                                                     _ -> pure () -- TODO: this would report a command argument error for the set command
                                         "get"  -> case xs of
                                                     (key : _) -> do
                                                       val <- getMemoryStoreVal store key
                                                       case val of
                                                         Nothing -> send sock encodeNullBulkString
                                                         Just (MemoryStoreEntry (MSStringVal v) Nothing) -> send sock $ encodeBulkString v
                                                         Just (MemoryStoreEntry (MSStringVal v) (Just (T.ExpireDuration exDur, T.ExpireReference exRef))) -> do
                                                           hasPassed <- U.hasElapsedSince exDur exRef
                                                           if hasPassed then do
                                                             _ <- delMemoryStoreKey store key
                                                             send sock encodeNullBulkString
                                                           else send sock $ encodeBulkString v
                                         "rpush" -> handleRPushCommand xs store sock
                                         "lpush" -> handleLPushCommand xs store sock
                                         "lrange" -> case xs of
                                                       (key : start : stop : _) -> do
                                                         val <- getMemoryStoreVal store key
                                                         case val of
                                                           Nothing -> send sock $ encodeArray []
                                                           Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
                                                             let itemCount = length vs
                                                             case (U.bsToInt start, U.bsToInt stop) of
                                                               (Just s, Just st) -> send sock $ encodeArray $ go vs (normStart s) (normStop st)
                                                                                    where go :: [BS.ByteString] -> Int -> Int -> [BS.ByteString]
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
                                                               _                 -> pure () -- TODO: this would report an error about converting the arg to an int
                                                       _ -> pure () -- TODO: this would report a command argument error for the rpush command
                                         "llen" -> case xs of
                                                     (key : _) -> do
                                                       val <- getMemoryStoreVal store key
                                                       case val of
                                                         Nothing -> send sock $ encodeInteger 0
                                                         Just (MemoryStoreEntry (MSListVal v) Nothing) -> send sock $ encodeInteger $ length v
                                                     _ -> pure () -- TODO: this would report a command argument error for the llen command
                                         "lpop" -> handleLPopCommand xs store sock
                                         _           -> pure () -- TODO: this would report a command error

main :: IO ()
main = do

    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering

    -- You can use print statements as follows for debugging, they'll be visible when running tests.
    -- hPutStrLn stderr "Logs from your program will appear here"

    store <- newMemoryStore

    let port = "6379"
    putStrLn $ "Redis server listening on port " ++ port
    serve HostAny port $ \(socket, address) -> do
        putStrLn $ "successfully connected client: " ++ show address

        let loop = do
              received <- recv socket 4096
              case received of
                Nothing -> pure ()
                Just raw_command -> case parseCommand raw_command of
                    Right message -> handleCommand socket store message >> loop
                    Left _ -> loop

        loop
        closeSock socket
