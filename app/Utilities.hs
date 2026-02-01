module Utilities
  ( bsToLower
  , bsToInteger
  , bsToInt
  , nowNs
  , hasElapsedSince
  ) where

import Control.Monad (guard)
import Data.Char (toLower)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import System.Clock (getTime, toNanoSecs, Clock (Monotonic))

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

nowNs :: IO Integer
nowNs = toNanoSecs <$> getTime Monotonic

elapsedMs :: Integer -> Integer -> Integer
elapsedMs start end = (end - start) `div` 1000000

-- example
hasElapsedSince :: Integer -> Integer -> IO Bool
hasElapsedSince thresholdMs startNs = do
  endNs <- nowNs
  pure (elapsedMs startNs endNs >= thresholdMs)
