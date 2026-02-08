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
  , randomAlphaNum40BS
  , decodeRdbBase64
  , emptyRdbFile
  ) where

import System.Random.Stateful (uniformRM, globalStdGen)

import Control.Monad (replicateM, guard)
import Data.Char (toLower)
import Text.Megaparsec (errorBundlePretty,  ParseErrorBundle)
import Text.Read (readMaybe)

import Data.Void (Void)
import Data.Bits ((.&.))

import qualified Data.Map.Strict as M

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Base64.URL as B64URL
import qualified Data.ByteString.Lazy as BSL

import Types (EntryId (..))

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
entryIdToBS EntryGenNew =
  BSL.toStrict $
    BB.toLazyByteString $
      BB.char7 '*' <> BB.char7 '-' <> BB.char7 '*'
entryIdToBS (EntryGenSeq p) =
  BSL.toStrict $
    BB.toLazyByteString $
      BB.word64Dec p <> BB.char7 '-' <> BB.char7 '*'
entryIdToBS (EntryId ms seq) =
  BSL.toStrict $
    BB.toLazyByteString $
      BB.word64Dec ms <> BB.char7 '-' <> BB.word64Dec seq

renderParseError :: ParseErrorBundle BS.ByteString Void -> BS.ByteString
renderParseError =
 BS8.pack . take 200 . oneLine . errorBundlePretty
 where
  oneLine = map (\c -> if c == '\n' || c == '\r' then ' ' else c)

randomAlphaNum40BS :: IO BS.ByteString
randomAlphaNum40BS = BS8.pack <$> replicateM 40 pick
  where
    alphabet = ['0'..'9'] <> ['A'..'Z'] <> ['a'..'z']
    pick = (alphabet !!) <$> uniformRM (0, length alphabet - 1) globalStdGen

decodeRdbBase64 :: BS.ByteString -> Either String BS.ByteString
decodeRdbBase64 input =
  let cleaned = pad (BS.filter (not . isWS) input)
  in case B64.decode cleaned of
       Right x -> Right x
       Left _  -> B64URL.decode cleaned
  where
    isWS w = w == 9 || w == 10 || w == 13 || w == 32
    pad bs =
      let r = BS.length bs .&. 3
      in bs <> case r of
           0 -> ""
           2 -> "=="
           3 -> "="
           _ -> bs

emptyRdbFile :: BS.ByteString
emptyRdbFile = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
