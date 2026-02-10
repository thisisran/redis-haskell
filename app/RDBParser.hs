{-# LANGUAGE OverloadedStrings #-}

module RDBParser
  ( consumeMetadata
  , consumeDB
  ) where

import Data.Time.Clock.POSIX (getPOSIXTime)

import System.IO (withBinaryFile, IOMode(ReadMode), Handle, SeekMode(RelativeSeek), hSeek)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import Control.Concurrent.STM (atomically, modifyTVar', TVar)
import qualified Data.Map.Strict as M

import Control.Monad (when)
import Data.Bits
import Data.Word

import qualified Utilities as U
import Types (SetExpiry (..), LengthEncoding (..), MemoryStore (..), MemoryStoreEntry (..), MemoryStoreValue (..), ExpireDuration (..), ExpireReference (..))

twoBytesToInt :: Word8 -> Word8 -> Int
twoBytesToInt hi lo =
  fromIntegral ((fromIntegral hi :: Word16) `shiftL` 8 .|. fromIntegral lo)

fourBytesToInt :: Word8 -> Word8 -> Word8 -> Word8 -> Int
fourBytesToInt hi1 hi2 lo1 lo2 =
  fromIntegral ((fromIntegral hi1 :: Word32) `shiftL` 24 .|.
                (fromIntegral hi2 `shiftL` 16) .|.
                (fromIntegral lo1 `shiftL` 8) .|.
                fromIntegral lo2)

eightBytesToInt :: Word8 -> Word8 -> Word8 -> Word8 -> Word8 -> Word8 -> Word8 -> Word8 -> Int
eightBytesToInt w1 w2 w3 w4 w5 w6 w7 w8 =
  (fromIntegral w1 `shiftL` 56) .|.
  (fromIntegral w2 `shiftL` 48) .|.
  (fromIntegral w3 `shiftL` 40) .|.
  (fromIntegral w4 `shiftL` 32) .|.
  (fromIntegral w5 `shiftL` 24) .|.
  (fromIntegral w6 `shiftL` 16) .|.
  (fromIntegral w7 `shiftL`  8) .|.
   fromIntegral w8

-- not for Integer strings, or compressed strings
lengthEncoding :: Handle -> IO LengthEncoding
lengthEncoding h = do
  lenc1 <- BS.hGet h 1
  let byte1 = BS.head lenc1
  case byte1 `shiftR` 6 of
    -- print "the next 6 bits represent the length"
    0 -> pure (SimpleString $ fromIntegral byte1)
    1 -> do
      lenc2 <- BS.hGet h 1
      pure (SimpleString $ twoBytesToInt byte1 (BS.head lenc2))
    2 -> do
      part1 <- BS.hGet h 1
      part2 <- BS.hGet h 1
      part3 <- BS.hGet h 1
      part4 <- BS.hGet h 1
      pure (SimpleString $ fourBytesToInt (BS.head part1) (BS.head part2) (BS.head part3) (BS.head part4))
    _ -> pure $ ComplexString $ fromIntegral (byte1 .&. 0x37)

decodeString :: Handle -> IO (Maybe BS.ByteString)
decodeString h = do
  mCount <- lengthEncoding h
  case mCount of
    SimpleString count -> do
      str <- BS.hGet h count
      pure $ Just str
    ComplexString b -> do
      case b of
           -- Integers as strings
           0 -> do
             num1c <- BS.hGet h 1
             let num1 = fromIntegral $ BS.head num1c :: Int
             pure $ Just (BS8.pack . show $ num1)
           1 -> do
             num1c <- BS.hGet h 1
             num2c <- BS.hGet h 1
             pure $ Just (BS8.pack . show $ twoBytesToInt (BS.head num1c) (BS.head num2c))
           2 -> do
             num1c <- BS.hGet h 1
             num2c <- BS.hGet h 1
             num3c <- BS.hGet h 1
             num4c <- BS.hGet h 1
             pure $ Just (BS8.pack . show $ fourBytesToInt (BS.head num1c) (BS.head num2c) (BS.head num3c) (BS.head num4c))
           -- compressed string
           _ -> do
             clenP <- lengthEncoding h
             case clenP of
               SimpleString clen -> do
                 ulenP <- lengthEncoding h
                 case ulenP of
                   SimpleString ulen -> do
                     compressed <- BS.hGet h ulen
                     undefined -- TODO (not implemented throughout the course): (decompress compressed using LZF)
                   _ -> pure Nothing -- By the specification, we won't get here
               _ -> pure Nothing -- By the specification, we won't get here

consumeMetadata :: Handle -> IO ()
consumeMetadata h = do
  go True h
  where
    go False h = do
      hSeek h RelativeSeek (-1)
      pure ()
    go True h = do
     firstByte <- BS.hGet h 1
     let isMDSection = BS.head firstByte == 250
     when isMDSection processMetadata
     go isMDSection h
    processMetadata = do
      mKey <- decodeString h
      case mKey of
        Just key -> do
          mValue <- decodeString h
          case mValue of
            Just value -> print $ key <> ": " <> value

-- consumes only 1 hash table
consumeHashTable :: Handle -> TVar (M.Map BS.ByteString MemoryStoreEntry) -> IO ()
consumeHashTable h store = do
  firstByte <- BS.hGet h 1
  let isHashTable = BS.head firstByte == 251
  when isHashTable processHashTableEntry
  where
    processHashTableEntry = do
      mTotalEntries <- lengthEncoding h
      case mTotalEntries of
        SimpleString totalEntries -> do
          mExpiryEntries <- lengthEncoding h
          case mExpiryEntries of
            SimpleString expiryEntries -> do
              -- print $ "Total entries = " <> (BS8.pack . show) totalEntries <> ", Expired keys = " <> (BS8.pack . show) expiryEntries
              processNormalKey $ totalEntries - expiryEntries
              processExpiredKey expiryEntries
            _ -> pure ()
        _ -> pure ()
    processNormalKey 0 = pure ()
    processNormalKey count = do
      fByte <- BS.hGet h 1
      when (BS.head fByte == 0) $ do -- currently only supporting string values
        (readKey, readValue) <- processKeyValue
        let formattedValue = MemoryStoreEntry (MSStringVal readValue) Nothing
        atomically $ modifyTVar' store (M.insert readKey formattedValue)
        -- print $ "Key: " <> readKey <> ", Value: " <> readValue
        processNormalKey $ count - 1
    processExpiredKey 0 = pure ()
    processExpiredKey count = do
      fByte <- BS.hGet h 1
      expiry <- if BS.head fByte == 252
                then processMilliKey
                else if BS.head fByte == 253 then processSecondsKey else pure 0 -- expiry is in milliseconds, on error just return 0 as expiry
      vType <- BS.hGet h 1
      when (BS.head vType == 0) $ do -- currently only supporting string values
        (readKey, readValue) <- processKeyValue
        now <- U.nowNs
        print $ "Now: " <> show now
        let dur = max 0 (expiry - fromIntegral now)
        print $ "Expiry: " <> show expiry
        let formattedValue = MemoryStoreEntry (MSStringVal readValue) (Just (ExpireDuration (fromIntegral dur), ExpireReference now))
        atomically $ modifyTVar' store (M.insert readKey formattedValue)
        -- print $ "Key: " <> readKey <> ", Value: " <> readValue <> ", Expiry: " <> (BS8.pack . show) expiry
      processExpiredKey $ count - 1
    processSecondsKey = do
      num1c <- BS.hGet h 1
      num2c <- BS.hGet h 1
      num3c <- BS.hGet h 1
      num4c <- BS.hGet h 1
      pure $ fourBytesToInt (BS.head num4c) (BS.head num3c) (BS.head num2c) (BS.head num1c) * 1_000
    processMilliKey = do
      num1c <- BS.hGet h 1
      num2c <- BS.hGet h 1
      num3c <- BS.hGet h 1
      num4c <- BS.hGet h 1
      num5c <- BS.hGet h 1
      num6c <- BS.hGet h 1
      num7c <- BS.hGet h 1
      num8c <- BS.hGet h 1
      pure $ eightBytesToInt (BS.head num8c) (BS.head num7c) (BS.head num6c) (BS.head num5c)  (BS.head num4c) (BS.head num3c) (BS.head num2c) (BS.head num1c)
    processKeyValue = do
      mKey <- decodeString h
      case mKey of
        Just key -> do
          mValue <- decodeString h
          case mValue of
            Just value -> pure (key, value)
            _ -> pure ("","")
        _ -> pure ("","")

consumeDB :: Handle -> TVar (M.Map BS.ByteString MemoryStoreEntry) -> IO ()
consumeDB h store = do
  go True h
  where
    go False h = do
      hSeek h RelativeSeek (-1)
      pure ()
    go True h = do
      firstByte <- BS.hGet h 1
      let isDBSection = BS.head firstByte == 254
      when isDBSection processDatabaseEntry
      go isDBSection h
    processDatabaseEntry = do
      mCount <- lengthEncoding h
      case mCount of
        SimpleString count -> do
          print $ "Index: " <> (BS8.pack. show) count
          consumeHashTable h store -- TODO: check what type of item is presented, for the challenge we assume a hash table
          -- mEndOfFile <- BS.hGet h 1
        _ -> pure () -- should not be the case according to the spec

-- main :: IO ()
-- main =
--   withBinaryFile "dump.rdb" ReadMode $ \h -> do
--     magicWord <- BS.hGet h 5
--     redisVersion <- BS.hGet h 4
--     print $ "Header section: " <> magicWord <> " " <> redisVersion
--     consumeMetadata h
--     consumeDB h
