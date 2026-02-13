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
import Types (RespError (..))

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

encodeSimpleError :: RespError -> BS.ByteString -> BS.ByteString
encodeSimpleError RErrXAddGtThan0 _ = "-" <> "ERR " <> "The ID specified in XADD must be greater than 0-0" <> "\r\n"
encodeSimpleError RErrXaddEqSmallTargetItem _ = "-" <> "ERR " <> "The ID specified in XADD is equal or smaller than the target stream top item"
encodeSimpleError RErrXRangeIDNonExisting _ = "-" <> "ERR " <> "XRANGE: The end id does not exist"
encodeSimpleError RErrIncrNotIntegerOrRange _ = "-" <> "ERR " <> "value is not an integer or out of range"
encodeSimpleError RErrExecNoMulti _ = "-" <> "ERR " <> "EXEC without MULTI"
encodeSimpleError RErrDiscardNoMulti _ = "-" <> "ERR " <> "DISCARD without MULTI"
encodeSimpleError RErrGeoAddLongRange _ = "-" <> "ERR " <> "longitude should be between -180.0 and 180.0 degrees"
encodeSimpleError RErrGeoAddLatRange _ = "-" <> "ERR " <> "latitude should be between -85.05112878 and 85.05112878 degrees"
encodeSimpleError RErrGeoDistMissingMember _ = "-" <> "ERR " <> "GeoDist: one of the members does not exist"
encodeSimpleError RErrAuthInvalidUserName _ = "-" <> "WRONGPASS " <> "invalid username-password pair or user is disabled"
encodeSimpleError RErrAuthServerAuthUserNotFound _ = "-" <> "ERR " <> "Server is authenticated but user not found."
encodeSimpleError RErrAuthRequired _ = "-" <> "NOAUTH " <> "Authentication required"
encodeSimpleError RErrSubUnauthorizedCmd cmdName = "-" <> "ERR " <> "Can't execute '" <> cmdName <> "' when one or more subscriptions exist"
