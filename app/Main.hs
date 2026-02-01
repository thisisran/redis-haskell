{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Network.Simple.TCP (serve, recv, send, HostPreference(HostAny), closeSock, Socket)
import System.IO (hPutStrLn, hSetBuffering, stdout, stderr, BufferMode(NoBuffering))

import Parser (parseCommand)
import qualified Types as T
import MemoryStore
import Encode (encodeBulkString, encodeNullBulkString, encodeSimpleString, encodeInteger)
import qualified Utilities as U

import qualified Data.ByteString as BS

handleCommand :: Socket -> MemoryStore -> [Maybe BS.ByteString] -> IO ()
handleCommand sock store [] = pure ()
handleCommand sock store (Just x:xs) = case U.bsToLower x of
                                         "ping" -> send sock $ encodeSimpleString "PONG"
                                         "echo" -> case xs of
                                                     (Just arg : _) -> send sock $ encodeBulkString arg
                                                     _              -> pure () -- TODO: this would report a command argument error
                                         "set"  -> case xs of
                                                     (Just key : Just val : xs) -> do
                                                       now <- U.nowNs
                                                       setMemoryStoreKey store key $ handleArguments val xs now
                                                       send sock $ encodeSimpleString "OK"
                                                       where
                                                         -- TODO: Eventually will need to turn it into a recursive function that will process all the arguments provided
                                                         -- Not just 1 additional one
                                                         handleArguments :: BS.ByteString -> [Maybe BS.ByteString] -> Integer -> MemoryStoreEntry
                                                         handleArguments v (Just y1 : Just y2 : _) now = case U.bsToLower y1 of
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
                                                     (Just key : _) -> do
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
                                         "rpush" -> case xs of
                                                      (Just key : Just elem : _) -> do -- TODO: implement support for adding multiple values at the same time
                                                        val <- getMemoryStoreVal store key
                                                        case val of
                                                             Nothing -> do
                                                               setMemoryStoreKey store key (MemoryStoreEntry (MSListVal [elem]) Nothing)
                                                               send sock $ encodeInteger 1
                                                             Just (MemoryStoreEntry (MSListVal vs) Nothing) -> do
                                                               setMemoryStoreKey store key (MemoryStoreEntry (MSListVal (vs ++ [elem])) Nothing)
                                                               send sock $ encodeInteger (length vs + 1)
                                                      _ -> pure () -- TODO: this would report a command argument error for the rpush command
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
