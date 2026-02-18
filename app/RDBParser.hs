{-# LANGUAGE OverloadedStrings #-}

module RDBParser
  ( consumeMetadata
  , consumeDB
  ) where

import Control.Monad (void, when)
import Control.Monad.Trans.Except (ExceptT (..), runExceptT)
import Control.Monad.Except (throwError, MonadError)

import System.IO (Handle, SeekMode(RelativeSeek), hSeek)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Maybe (MaybeT (..), runMaybeT, hoistMaybe)

import Control.Concurrent.STM (atomically, modifyTVar', TVar)
import qualified Data.Map.Strict as M

import Data.Bits

import qualified Utilities as U
import Types (LengthEncoding (..), StoreEntry (..), StoreValue (..), ExDurationMs (..), ExRef (..), AppError (..))

type RDBParser = ExceptT AppError IO

-- not for Integer strings, or compressed strings
lengthEncoding :: Handle -> RDBParser LengthEncoding
lengthEncoding h = do
  lenc1 <- liftIO $ BS.hGet h 1
  case BS.uncons lenc1 of
    Just (byte1, _) -> case byte1 `shiftR` 6 of
      -- "the next 6 bits represent the length"
      0 -> pure $ SimpleString $ fromIntegral byte1
      1 -> do
        lenc2 <- liftIO $ BS.hGet h 1
        case BS.uncons lenc2 of
          Just (byte2, _) -> pure $ SimpleString $ U.twoBytesToInt byte1 byte2
          Nothing -> throwError $ ErrRDBParser "Error (RDB Parser): Error parsing length encoding (second byte)"
      2 -> do
        bs <- liftIO $ BS.hGet h 4
        case BS.unpack bs of
          [x1,x2,x3,x4] -> pure $ SimpleString $ U.fourBytesToInt x1 x2 x3 x4
          _ -> throwError $ ErrRDBParser "Error (RDB Parser): Error parsing length encoding (1 -> 4 bytes)"
      _ -> pure $ ComplexString $ fromIntegral (byte1 .&. 0x37)
    Nothing -> throwError $ ErrRDBParser "Error (RDB Parser): Error parsing length encoding (first byte)"

decodeString :: Handle -> RDBParser BS.ByteString
decodeString h = do
  mCount <- lengthEncoding h
  case mCount of
    SimpleString count -> do
      liftIO $ BS.hGet h count
    ComplexString b -> do
      case b of
           -- Integers as strings
           0 -> do
             num1c <- liftIO $ BS.hGet h 1
             case BS.uncons num1c of
               Just (x, _) -> pure $ (BS8.pack . show) x
               Nothing -> throwError $ ErrRDBParser "Error (RDB Parser): Error parsing a 1 byte integer as string"
           1 -> do
             bs <- liftIO $ BS.hGet h 2
             case BS.unpack bs of
               [x1,x2] -> pure $ (BS8.pack . show) (U.twoBytesToInt x1 x2)
               _ -> throwError $ ErrRDBParser "Error (RDB Parser): Error parsing a 2 byte integer as string"
           2 -> do
             bs <- liftIO $ BS.hGet h 4
             case BS.unpack bs of
               [x1, x2, x3, x4] -> pure $ (BS8.pack . show) (U.fourBytesToInt x1 x2 x3 x4)
               _ -> throwError $ ErrRDBParser "Error (RDB Parser): Error parsing a 4 byte integer as string"
           -- compressed string
           _ -> do
             clenP <- lengthEncoding h
             case clenP of
               SimpleString clen -> do
                 ulenP <- lengthEncoding h
                 case ulenP of
                   SimpleString ulen -> throwError $ ErrRDBParser "Error (RDB Parser): complex string LZF decompression not supported." -- TODO (not implemented throughout the course)
                     -- compressed <- BS.hGet h ulen
                   _ -> throwError $ ErrRDBParser "Error (RDB Parser): unknown error in compressed simple string parsing" -- By the specification, we won't get here
               _ -> throwError $ ErrRDBParser "Error (RDB Parser): unknown error in compressed complex string parsing" -- By the specification, we won't get here
 
consumeMetadata :: Handle -> RDBParser ()
consumeMetadata h = do
  go True h
  where
    go :: Bool -> Handle -> RDBParser ()
    go False h = do
      liftIO $ hSeek h RelativeSeek (-1)
      pure ()
    go True h = do
     firstByte <- liftIO $ BS.hGet h 1
     case BS.uncons firstByte of
       Just (x, _) -> do
           let isMDSection = x == 250
           when isMDSection processMetadata
           go isMDSection h
       Nothing -> pure ()
    processMetadata = do
      void $ decodeString h
      void $ decodeString h

-- consumes only 1 hash table
consumeHashTable :: Handle -> TVar (M.Map BS.ByteString StoreEntry) -> RDBParser ()
consumeHashTable h store = do
  firstByte <- liftIO $ BS.hGet h 1
  case BS.uncons firstByte of
    Just (x, _) -> do
      let isHashTable = x == 251
      when isHashTable processHashTableEntry
    Nothing -> throwError $ ErrRDBParser "Error (RDB Parser): error parsing hashtable header"
  where
    processHashTableEntry = do
      mTotalEntries <- lengthEncoding h
      case mTotalEntries of
        SimpleString totalEntries -> do
          mExpiryEntries <- lengthEncoding h
          case mExpiryEntries of
            SimpleString expiryEntries -> do
              processNormalKey $ totalEntries - expiryEntries
              processExpiredKey expiryEntries
            _ -> throwError $ ErrRDBParser "Error (RDB Parser): error parsing hashtable expired entries count"
        _ -> throwError $ ErrRDBParser "Error (RDB Parser): error parsing hashtable total entries count"
    processNormalKey 0 = pure ()
    processNormalKey count = do
      fByte <- liftIO $ BS.hGet h 1
      case BS.uncons fByte of
        Just (x, _) -> when (x == 0) $ do -- currently only supporting string values
          (readKey, readValue) <- processKeyValue
          let formattedValue = StoreEntry (StoreString readValue) Nothing
          liftIO $ atomically $ modifyTVar' store (M.insert readKey formattedValue)
          processNormalKey $ count - 1
        _ -> throwError $ ErrRDBParser "Error (RDB Parser): error parsing hashtable total entries count"
    processExpiredKey :: Int -> RDBParser ()
    processExpiredKey 0 = pure ()
    processExpiredKey count = do
      fByte <- liftIO $ BS.hGet h 1
      case BS.uncons fByte of
        Just (x, _) -> do
          expiry <- if x == 252
                    then processMilliKey
                    else if x == 253 then processSecondsKey else pure 0 -- expiry is in milliseconds, on error just return 0 as expiry
          vType <- liftIO $ BS.hGet h 1
          case BS.uncons vType of
            Just (y, _) -> when (y == 0) $ do -- currently only supporting string values
              (readKey, readValue) <- processKeyValue
              now <- liftIO U.nowMS
              let dur = max 0 (expiry - fromIntegral now)
              let formattedValue = StoreEntry (StoreString readValue) (Just (ExDurationMs (fromIntegral dur), ExRef now))
              liftIO $ atomically $ modifyTVar' store (M.insert readKey formattedValue)
              processExpiredKey $ count - 1
            Nothing -> throwError $ ErrRDBParser "Error (RDB Parser): error processing type of expired key"
        _ -> throwError $ ErrRDBParser "Error (RDB Parser): error processing expired key"
    processSecondsKey :: RDBParser Int
    processSecondsKey = do
      bs <- liftIO $ BS.hGet h 4
      case BS.unpack bs of
        [x1, x2, x3, x4] -> pure $ U.fourBytesToInt x4 x3 x2 x1 * 1_000
        _ -> throwError $ ErrRDBParser "Error (RDB Parser): Error in parsing a 4 byte seconds expired key"
    processMilliKey :: RDBParser Int
    processMilliKey = do
       bs <- liftIO $ BS.hGet h 8
       case BS.unpack bs of
         [x1,x2,x3,x4,x5,x6,x7,x8] -> pure $ U.eightBytesToInt x8 x7 x6 x5 x4 x3 x2 x1
         _ -> throwError $ ErrRDBParser "Error (RDB Parser): Error in parsing an 8 byte milliseconds expired key"
    processKeyValue = do
      key <- decodeString h
      value <- decodeString h
      pure (key, value)

consumeDB :: Handle -> TVar (M.Map BS.ByteString StoreEntry) -> RDBParser ()
consumeDB h store = go True h
  where
    go False h = do
      liftIO $ hSeek h RelativeSeek (-1)
      pure ()
    go True h = do
      firstByte <- liftIO $ BS.hGet h 1
      case BS.uncons firstByte of
        Just (x, _) -> do
          let isDBSection = x == 254
          when isDBSection processDatabaseEntry
          go isDBSection h
        Nothing -> pure ()
    processDatabaseEntry = do
      mCount <- lengthEncoding h
      case mCount of
        SimpleString count -> consumeHashTable h store -- TODO: check what type of item is presented, for the challenge we assume a hash table
          -- mEndOfFile <- BS.hGet h 1
        _ -> throwError $ ErrRDBParser "Error (RDB Parser): Error in parsing database header" -- should not be the case according to the spec
