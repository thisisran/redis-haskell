module Utilities
  ( bsToLower
  , bsToInteger
  , bsToInt
  , bsToDouble
  , nowNs
  , hasElapsedSince
  , entryIdToBS
  , range
  , renderParseError
  ) where

import Control.Monad (guard)
import Data.Char (toLower)
import Text.Megaparsec (errorBundlePretty,  ParseErrorBundle)
import Text.Read (readMaybe)

import Data.Void (Void)

import qualified Data.Map.Strict as M

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BSL

import MemoryStore (EntryId (..))

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

entryIdToBS :: EntryId -> BS.ByteString
entryIdToBS (EntryId ms seq) =
  BSL.toStrict $
    BB.toLazyByteString $
      BB.word64Dec ms <> BB.char7 '-' <> BB.word64Dec seq

renderParseError :: ParseErrorBundle BS.ByteString Void -> BS.ByteString
renderParseError =
 BS8.pack . take 200 . oneLine . errorBundlePretty
 where
  oneLine = map (\c -> if c == '\n' || c == '\r' then ' ' else c)
