{-# LANGUAGE OverloadedStrings #-}

module Encode
  ( encodeBulkString
  , encodeNullBulkString
  , encodeSimpleString
  , encodeInteger
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

encodeBulkString :: BS.ByteString -> BS.ByteString
encodeBulkString x = "$" <> BS8.pack (show (BS.length x)) <> "\r\n" <> x <> "\r\n"

encodeNullBulkString :: BS.ByteString
encodeNullBulkString = "$-1\r\n"

encodeSimpleString :: BS.ByteString -> BS.ByteString
encodeSimpleString x = "+" <> x <> "\r\n"

encodeInteger :: Int -> BS.ByteString
encodeInteger x
  | x >= 0 = ":" <> (BS8.pack . show) x <> "\r\n"
  | otherwise = ":-" <> (BS8.pack . show) (-x) <> "\r\n"
