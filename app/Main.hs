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

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

type Parser = Parsec Void BS.ByteString

-- data RESPElement = BulkString Text | Number Int | Array RESPElement deriving Show

bulkString :: Parser (Maybe BS.ByteString)
bulkString = do
  void (B.char 36) -- '$'
  n <- L.signed (pure ()) L.decimal
  B.crlf
  if n == (-1) then pure Nothing
  else Just <$> takeP (Just "bulk string") n <* B.crlf

array :: Parser [Maybe BS.ByteString]
array = do
  void (B.char 42) -- '*'
  n <- L.decimal
  B.crlf
  count n bulkString

formatBulkString :: BS.ByteString -> BS.ByteString
formatBulkString x = "$" <> BS8.pack (show (BS.length x)) <> "\r\n" <> x <> "\r\n"

handleCommand :: Socket -> [Maybe BS.ByteString] -> IO ()
handleCommand sock = \case
  (Just "PING" : _)          -> send sock "+PONG\r\n"
  (Just "ECHO" : Just x : _) -> send sock $ formatBulkString x
  _                          -> pure ()

main :: IO ()
main = do

    -- Disable output buffering
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering

    -- You can use print statements as follows for debugging, they'll be visible when running tests.
    -- hPutStrLn stderr "Logs from your program will appear here"

    -- Uncomment the code below to pass the first stage stage 1
    let port = "6379"
    putStrLn $ "Redis server listening on port " ++ port
    serve HostAny port $ \(socket, address) -> do
        putStrLn $ "successfully connected client: " ++ show address

        let loop = do
              received <- recv socket 4096
              case received of
                Nothing -> pure ()
                Just raw_command -> case parse array "" raw_command of
                    Right message -> handleCommand socket message >> loop
                    Left _ -> loop

        loop
        closeSock socket
