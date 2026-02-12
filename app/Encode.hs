{-# LANGUAGE OverloadedStrings #-}

module Encode
  ( encodeBulkString
  , encodeRdbFile
  , encodeNullBulkString
  , encodeSimpleString
  , encodeInteger
  , encodeArray
  , encodeNullArray
  , encodeEmptyArray
  , encodeSimpleError
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

encodeBulkString :: BS.ByteString -> BS.ByteString
encodeBulkString x = "$" <> (BS8.pack . show . BS.length) x <> "\r\n" <> x <> "\r\n"

encodeRdbFile :: BS.ByteString -> BS.ByteString
encodeRdbFile x = "$" <> (BS8.pack . show . BS.length) x <> "\r\n" <> x

encodeNullBulkString :: BS.ByteString
encodeNullBulkString = "$-1\r\n"

encodeSimpleString :: BS.ByteString -> BS.ByteString
encodeSimpleString x = "+" <> x <> "\r\n"

encodeInteger :: Int -> BS.ByteString
encodeInteger x
  | x >= 0 = ":" <> (BS8.pack . show) x <> "\r\n"
  | otherwise = ":-" <> (BS8.pack . show) (-x) <> "\r\n"

encodeNullArray :: BS.ByteString
encodeNullArray = "*-1\r\n"

encodeEmptyArray :: BS.ByteString
encodeEmptyArray = "*0\r\n"

encodeArray :: Bool  -> [BS.ByteString] -> BS.ByteString
encodeArray encodeStr xs
  | null xs = encodeEmptyArray
  | otherwise = "*" <> (BS8.pack . show . length) xs <> "\r\n" <> foldr (\x acc -> let curr = if encodeStr then encodeBulkString x else x in curr <> acc) BS.empty xs

encodeSimpleError :: BS.ByteString -> BS.ByteString
encodeSimpleError x = "-ERR " <> x <> "\r\n"
