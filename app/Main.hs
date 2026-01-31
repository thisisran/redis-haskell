{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (void)
import Network.Simple.TCP (serve, recv, send, HostPreference(HostAny), closeSock, Socket)
import System.IO (hPutStrLn, hSetBuffering, stdout, stderr, BufferMode(NoBuffering))

import qualified Data.Text as T
import qualified Data.Text.Encoding as TE

import Data.Void (Void)
import Text.Megaparsec (Parsec, parse, takeP, count)
import qualified Text.Megaparsec.Byte as B
import qualified Text.Megaparsec.Byte.Lexer as L

import Control.Concurrent.STM
import qualified Data.Map.Strict as M

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

type Parser = Parsec Void BS.ByteString

newtype Store = Store (TVar (M.Map BS.ByteString BS.ByteString))

-- data RESPElement = BulkString Text | Number Int | Array RESPElement deriving Show

-------------------------------------------------------------
-- Memory store

newStore :: IO Store
newStore = Store <$> newTVarIO M.empty

getStoreVal :: Store -> BS.ByteString -> IO (Maybe BS.ByteString)
getStoreVal (Store tv) k = M.lookup k <$> readTVarIO tv

setStoreKey :: Store -> BS.ByteString -> BS.ByteString -> IO ()
setStoreKey (Store tv) k v = atomically $ modifyTVar' tv (M.insert k v)

del :: Store -> BS.ByteString -> IO Bool
del (Store tv) k = atomically $ do
  m <- readTVar tv
  let existed = M.member k m
  writeTVar tv (M.delete k m)
  pure existed
  
-------------------------------------------------------------
-- Parser functions

parseBulkString :: Parser (Maybe BS.ByteString)
parseBulkString = do
  void (B.char 36) -- '$'
  n <- L.signed (pure ()) L.decimal
  B.crlf
  if n == (-1) then pure Nothing
  else Just <$> takeP (Just "bulk string") n <* B.crlf

parseArray :: Parser [Maybe BS.ByteString]
parseArray = do
  void (B.char 42) -- '*'
  n <- L.decimal
  B.crlf
  count n parseBulkString

-------------------------------------------------------------
-- Formatting functions

bulkString :: BS.ByteString -> BS.ByteString
bulkString x = "$" <> BS8.pack (show (BS.length x)) <> "\r\n" <> x <> "\r\n"

nullBulkString :: BS.ByteString
nullBulkString = "$-1\r\n"

formatSimpleString :: BS.ByteString -> BS.ByteString
formatSimpleString x = "+" <> x <> "\r\n"

-------------------------------------------------------------

handleCommand :: Socket -> Store -> [Maybe BS.ByteString] -> IO ()
handleCommand sock store = \case
  (Just "PING" : _)                       -> send sock $ formatSimpleString "PONG"
  (Just "ECHO" : Just x : _)              -> send sock $ bulkString x
  (Just "SET"  : Just key : Just val : _) -> do
    setStoreKey store key val
    send sock $ formatSimpleString "OK"
  (Just "GET" : Just key : _)             -> do
    val <- getStoreVal store key
    hPutStrLn stderr $ "Getting " <> BS8.unpack key <> " from store"
    case val of
      Nothing -> send sock nullBulkString
      Just x -> send sock $ bulkString x
  _                                       -> pure ()

main :: IO ()
main = do

    -- Disable output buffering
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering

    -- You can use print statements as follows for debugging, they'll be visible when running tests.
    -- hPutStrLn stderr "Logs from your program will appear here"

    store <- newStore
      
    -- Uncomment the code below to pass the first stage stage 1
    let port = "6379"
    putStrLn $ "Redis server listening on port " ++ port
    serve HostAny port $ \(socket, address) -> do
        putStrLn $ "successfully connected client: " ++ show address

        let loop = do
              received <- recv socket 4096
              case received of
                Nothing -> pure ()
                Just raw_command -> case parse parseArray "" raw_command of
                    Right message -> handleCommand socket store message >> loop
                    Left _ -> loop

        loop
        closeSock socket
