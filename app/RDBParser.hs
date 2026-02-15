{-# LANGUAGE OverloadedStrings #-}

module RDBParser
  ( consumeMetadata
  , consumeDB
  ) where

import Data.Time.Clock.POSIX (getPOSIXTime)

import System.IO (withBinaryFile, IOMode(ReadMode), Handle, SeekMode(RelativeSeek), hSeek)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT, hoistMaybe)

import Control.Concurrent.STM (atomically, modifyTVar', TVar)
import qualified Data.Map.Strict as M

import Data.Bits

import Control.Monad (when)

import qualified Utilities as U
import Types (SetExpiry (..), LengthEncoding (..), Store (..), StoreEntry (..), StoreValue (..), ExDurationMs (..), ExRef (..))

-- not for Integer strings, or compressed strings
lengthEncoding :: Handle -> IO (Either BS.ByteString LengthEncoding)
lengthEncoding h = do
  lenc1 <- BS.hGet h 1
  case BS.uncons lenc1 of
    Just (byte1, _) -> case byte1 `shiftR` 6 of
      -- "the next 6 bits represent the length"
      0 -> pure $ Right (SimpleString $ fromIntegral byte1)
      1 -> do
        lenc2 <- BS.hGet h 1
        case BS.uncons lenc2 of
          Just (byte2, _) -> pure $ Right (SimpleString $ U.twoBytesToInt byte1 byte2)
          Nothing -> pure $ Left "Error parsing length encoding (second byte)"
      2 -> do
        part1 <- BS.hGet h 1
        part2 <- BS.hGet h 1
        part3 <- BS.hGet h 1
        part4 <- BS.hGet h 1
        res <- runMaybeT $ do
          (p1, _) <- hoistMaybe $ BS.uncons part1
          (p2, _) <- hoistMaybe $ BS.uncons part2
          (p3, _) <- hoistMaybe $ BS.uncons part3
          (p4, _) <- hoistMaybe $ BS.uncons part4
          pure (p1, p2, p3, p4)
        case res of
          Just (p1, p2, p3, p4) -> pure $ Right (SimpleString $ U.fourBytesToInt p1 p2 p3 p4)
          Nothing -> pure $ Left "Error parsing length encoding (1 -> 4 bytes)"
      _ -> pure $ Right $ ComplexString $ fromIntegral (byte1 .&. 0x37)
    Nothing -> pure $ Left "Error parsing length encoding (first byte)"

decodeString :: Handle -> IO (Maybe BS.ByteString)
decodeString h = do
  mCount <- lengthEncoding h
  case mCount of
    Right (SimpleString count) -> do
      str <- BS.hGet h count
      pure $ Just str
    Right (ComplexString b) -> do
      case b of
           -- Integers as strings
           0 -> do
             num1c <- BS.hGet h 1
             case BS.uncons num1c of
               Just (x, _) -> pure $ Just (BS8.pack . show $ x)
               Nothing -> pure Nothing
           1 -> do
             num1c <- liftIO $ BS.hGet h 1
             num2c <- liftIO $ BS.hGet h 1
             res <- runMaybeT $ do
               (x1, _) <- hoistMaybe $ BS.uncons num1c
               (x2, _) <- hoistMaybe $ BS.uncons num2c
               pure (x1, x2)
             case res of
               Just (x1, x2) -> pure $ Just (BS8.pack . show $ U.twoBytesToInt x1 x2)
               Nothing -> pure Nothing
           2 -> do
             num1c <- BS.hGet h 1
             num2c <- BS.hGet h 1
             num3c <- BS.hGet h 1
             num4c <- BS.hGet h 1
             res <- runMaybeT $ do
               (x1, _) <- hoistMaybe $ BS.uncons num1c
               (x2, _) <- hoistMaybe $ BS.uncons num1c
               (x3, _) <- hoistMaybe $ BS.uncons num1c
               (x4, _) <- hoistMaybe $ BS.uncons num1c
               pure (x1, x2, x3, x4)
             case res of
               Just (x1, x2, x3, x4) -> pure $ Just (BS8.pack . show $ U.fourBytesToInt x1 x2 x3 x4)
               Nothing -> pure Nothing
           -- compressed string
           _ -> do
             clenP <- lengthEncoding h
             case clenP of
               Right (SimpleString clen) -> do
                 ulenP <- lengthEncoding h
                 case ulenP of
                   Right (SimpleString ulen) -> pure Nothing -- TODO (not implemented throughout the course): (decompress compressed using LZF)
                     -- compressed <- BS.hGet h ulen
                   _ -> pure Nothing -- By the specification, we won't get here
               _ -> pure Nothing -- By the specification, we won't get here
    Left _ -> pure Nothing
    
consumeMetadata :: Handle -> IO ()
consumeMetadata h = do
  go True h
  where
    go False h = do
      hSeek h RelativeSeek (-1)
      pure ()
    go True h = do
     firstByte <- BS.hGet h 1
     case BS.uncons firstByte of
       Just (x, _) -> do
           let isMDSection = x == 250
           when isMDSection processMetadata
           go isMDSection h
       Nothing -> pure ()
    processMetadata = do
      mKey <- decodeString h
      case mKey of
        Just key -> do
          mValue <- decodeString h
          case mValue of
            Just value -> pure ()

-- consumes only 1 hash table
consumeHashTable :: Handle -> TVar (M.Map BS.ByteString StoreEntry) -> IO ()
consumeHashTable h store = do
  firstByte <- BS.hGet h 1
  case BS.uncons firstByte of
    Just (x, _) -> do
      let isHashTable = x == 251
      when isHashTable processHashTableEntry
  where
    processHashTableEntry = do
      mTotalEntries <- lengthEncoding h
      case mTotalEntries of
        Right (SimpleString totalEntries) -> do
          mExpiryEntries <- lengthEncoding h
          case mExpiryEntries of
            Right (SimpleString expiryEntries) -> do
              processNormalKey $ totalEntries - expiryEntries
              processExpiredKey expiryEntries
            _ -> pure ()
        _ -> pure ()
    processNormalKey 0 = pure ()
    processNormalKey count = do
      fByte <- BS.hGet h 1
      case BS.uncons fByte of
        Just (x, _) -> when (x == 0) $ do -- currently only supporting string values
          (readKey, readValue) <- processKeyValue
          let formattedValue = StoreEntry (StoreString readValue) Nothing
          atomically $ modifyTVar' store (M.insert readKey formattedValue)
          processNormalKey $ count - 1
        Nothing -> pure ()
    processExpiredKey 0 = pure ()
    processExpiredKey count = do
      fByte <- BS.hGet h 1
      case BS.uncons fByte of
        Just (x, _) -> do
          expiry <- if x == 252
                    then processMilliKey
                    else if x == 253 then processSecondsKey else pure 0 -- expiry is in milliseconds, on error just return 0 as expiry
          vType <- BS.hGet h 1
          case BS.uncons vType of
            Just (y, _) -> when (y == 0) $ do -- currently only supporting string values
              (readKey, readValue) <- processKeyValue
              now <- U.nowMS
              let dur = max 0 (expiry - fromIntegral now)
              let formattedValue = StoreEntry (StoreString readValue) (Just (ExDurationMs (fromIntegral dur), ExRef now))
              atomically $ modifyTVar' store (M.insert readKey formattedValue)
              processExpiredKey $ count - 1
            Nothing -> pure ()
        Nothing -> pure ()
    processSecondsKey = do
      num1c <- BS.hGet h 1
      num2c <- BS.hGet h 1
      num3c <- BS.hGet h 1
      num4c <- BS.hGet h 1
      res <- runMaybeT $ do
        (x4, _) <- hoistMaybe $ BS.uncons num4c
        (x3, _) <- hoistMaybe $ BS.uncons num4c
        (x2, _) <- hoistMaybe $ BS.uncons num4c
        (x1, _) <- hoistMaybe $ BS.uncons num4c
        pure (x4, x3, x2, x1)
      case res of
        Just (x4, x3, x2, x1) -> pure $ U.fourBytesToInt x4 x3 x2 x1 * 1_000
        Nothing  -> pure 0
    processMilliKey = do
      num1c <- BS.hGet h 1
      num2c <- BS.hGet h 1
      num3c <- BS.hGet h 1
      num4c <- BS.hGet h 1
      num5c <- BS.hGet h 1
      num6c <- BS.hGet h 1
      num7c <- BS.hGet h 1
      num8c <- BS.hGet h 1
      res <- runMaybeT $ do
        (x8, _) <- hoistMaybe $ BS.uncons num8c
        (x7, _) <- hoistMaybe $ BS.uncons num7c
        (x6, _) <- hoistMaybe $ BS.uncons num6c
        (x5, _) <- hoistMaybe $ BS.uncons num5c
        (x4, _) <- hoistMaybe $ BS.uncons num4c
        (x3, _) <- hoistMaybe $ BS.uncons num3c
        (x2, _) <- hoistMaybe $ BS.uncons num2c
        (x1, _) <- hoistMaybe $ BS.uncons num1c
        pure (x8, x7, x6, x5, x4, x3, x2, x1)
      case res of
        Just (x8, x7, x6, x5, x4, x3, x2, x1) -> pure $ U.eightBytesToInt x8 x7 x6 x5 x4 x3 x2 x1
        Nothing  -> pure 0
    processKeyValue = do
      res <- runMaybeT $ do
        key <- MaybeT . liftIO $ decodeString h
        value <- MaybeT . liftIO $ decodeString h
        pure (key, value)
      case res of
        Just (k, v) -> pure (k, v)
        Nothing   -> pure (mempty, mempty)

consumeDB :: Handle -> TVar (M.Map BS.ByteString StoreEntry) -> IO ()
consumeDB h store = do
  go True h
  where
    go False h = do
      hSeek h RelativeSeek (-1)
      pure ()
    go True h = do
      firstByte <- BS.hGet h 1
      case BS.uncons firstByte of
        Just (x, _) -> do
          let isDBSection = x == 254
          when isDBSection processDatabaseEntry
          go isDBSection h
        Nothing -> pure ()
    processDatabaseEntry = do
      mCount <- lengthEncoding h
      case mCount of
        Right (SimpleString count) -> consumeHashTable h store -- TODO: check what type of item is presented, for the challenge we assume a hash table
          -- mEndOfFile <- BS.hGet h 1
        _ -> pure () -- should not be the case according to the spec
