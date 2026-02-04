{-# LANGUAGE NumericUnderscores #-}

module Utilities
  ( bsToLower
  , bsToInteger
  , bsToInt
  , bsToDouble
  , nowNs
  , hasElapsedSince
  , range
  ) where

import Control.Monad (guard)
import Data.Char (toLower)

import Text.Read (readMaybe)

import qualified Data.Map.Strict     as M

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import Data.Time.Clock.POSIX (getPOSIXTime)

bsToLower :: BS.ByteString -> BS.ByteString
bsToLower = BS8.map toLower

bsToInteger :: BS.ByteString -> Maybe Integer
bsToInteger bs = do
  (n, rest) <- BS8.readInteger bs   -- parses a signed decimal prefix
  guard (BS8.null rest)             -- require full consumption
  pure n

bsToInt :: BS.ByteString -> Maybe Int
bsToInt bs = do
  (n, rest) <- BS8.readInt bs   -- parses a signed decimal prefix
  guard (BS8.null rest)         -- require full consumption
  pure n

bsToDouble :: BS.ByteString -> Maybe Double
bsToDouble = readMaybe . BS8.unpack

nowNs :: IO Integer
nowNs = fmap (floor . (* 1000)) getPOSIXTime

hasElapsedSince :: Integer -> Integer -> IO Bool
hasElapsedSince thresholdMs startNs = do
  endNs <- nowNs
  pure (endNs - startNs >= thresholdMs)

range :: Ord k => (k -> k -> Bool) -> k -> k -> M.Map k v -> M.Map k v
range f lo hi m
  | lo > hi   = M.empty
  | otherwise = M.takeWhileAntitone (<= hi) $ M.dropWhileAntitone (`f` lo) m
