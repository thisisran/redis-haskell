{-# LANGUAGE OverloadedStrings #-}

module Parser
  ( parseCommand
  ) where

import Text.Megaparsec (Parsec, takeP, count, parse)
import qualified Text.Megaparsec.Byte as B
import qualified Text.Megaparsec.Byte.Lexer as L

import Data.Void (Void)
import Control.Monad (void)

import qualified Data.ByteString as BS

type Parser = Parsec Void BS.ByteString

-- data RESPElement = BulkString Text | Number Int | Array RESPElement deriving Show

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

parseCommand = parse parseArray ""
