module Utilities
  ( bsToLower
  , bsToInteger
  , bsToInt
  , bsToDouble
  , nowMS
  , hasElapsedSince
  , entryIdToBS
  , range
  , randomAlphaNum40BS
  , decodeRdbBase64
  , emptyRdbFile
  , spreadInt32ToInt64
  , twoBytesToInt
  , fourBytesToInt
  , eightBytesToInt
  , interleaveGeo
  , deinterleaveGeo
  , calcGeoDistance
  ) where

import System.Random.Stateful (uniformRM, globalStdGen)

import Control.Monad.IO.Class (liftIO)
                              
import Control.Monad (replicateM, guard)
import Data.Char (toLower)
import Text.Read (readMaybe)

import Data.Void (Void)

import Data.Bits
import Data.Word

import qualified Data.Map.Strict as M

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Base64 as B64
import qualified Data.ByteString.Base64.URL as B64URL
import qualified Data.ByteString.Lazy as BSL

import Types (EntryId (..), SetExpiry (..), StoreEntry (..), ExDurationMs (..),  ExRef (..), StoreValue (..))
import Store (MonadStore, setDataEntry)

import Data.Time.Clock.POSIX (getPOSIXTime)

minLatitude, maxLatitude, minLongitude, maxLongitude :: Double
minLatitude  = -85.05112878
maxLatitude  =  85.05112878
minLongitude = -180.0
maxLongitude =  180.0

latRange, lonRange :: Double
latRange = maxLatitude - minLatitude
lonRange = maxLongitude - minLongitude

scale26 :: Double
scale26 = 2^(26 :: Int)

normalizeTruncate :: Double -> Double -> (Word32, Word32)
normalizeTruncate lat lon = 
      let nlat = scale26 * (lat - minLatitude)  / latRange
          nlon = scale26 * (lon - minLongitude) / lonRange
          ilat = fromIntegral (floor nlat :: Integer) :: Word32
          ilon = fromIntegral (floor nlon :: Integer) :: Word32
      in (ilat, ilon)

spreadInt32ToInt64 :: Word32 -> Word64
spreadInt32ToInt64 v0 =
  let !v1 = fromIntegral v0 :: Word64
      !a  = (v1 .|. (v1 `shiftL` 16)) .&. 0x0000FFFF0000FFFF
      !b  = (a  .|. (a  `shiftL` 8 )) .&. 0x00FF00FF00FF00FF
      !c  = (b  .|. (b  `shiftL` 4 )) .&. 0x0F0F0F0F0F0F0F0F
      !d  = (c  .|. (c  `shiftL` 2 )) .&. 0x3333333333333333
      !e  = (d  .|. (d  `shiftL` 1 )) .&. 0x5555555555555555
  in e

interleaveGeo :: Double -> Double -> Double
interleaveGeo lat lon = let (normLat, normLong) = normalizeTruncate lat lon
                            !spreadX = spreadInt32ToInt64 normLat
                            !spreadY = spreadInt32ToInt64 normLong
                            !shiftedY = spreadY `shiftL` 1
                        in fromIntegral $ spreadX .|. shiftedY

compactInt64ToInt32 :: Word64 -> Word32
compactInt64ToInt32 v0 =
  let !a = v0 .&. 0x5555555555555555
      !b = (a  .|. (a  `shiftR` 1 )) .&. 0x3333333333333333
      !c = (b  .|. (b  `shiftR` 2 )) .&. 0x0F0F0F0F0F0F0F0F
      !d = (c  .|. (c  `shiftR` 4 )) .&. 0x00FF00FF00FF00FF
      !e = (d  .|. (d  `shiftR` 8 )) .&. 0x0000FFFF0000FFFF
      !f = (e  .|. (e  `shiftR` 16)) .&. 0x00000000FFFFFFFF
  in fromIntegral f

deinterleaveGeo :: Double -> (Double, Double)
deinterleaveGeo geo =
  let geoW = fromInteger (floor geo) :: Word64
      !x = geoW
      !y = geoW `shiftR` 1
      !gridLat = fromIntegral (compactInt64ToInt32 x) :: Double
      !gridLon = fromIntegral (compactInt64ToInt32 y) :: Double
      latMin = minLatitude  + latRange * (gridLat / scale26)
      latMax = minLatitude  + latRange * ((gridLat + 1) / scale26)
      lonMin = minLongitude + lonRange * (gridLon / scale26)
      lonMax = minLongitude + lonRange * ((gridLon + 1) / scale26)
  in ((latMin + latMax) / 2, (lonMin + lonMax) / 2)

degToRadius :: Double -> Double
degToRadius x = let pi = 3.14159265358979323846
                    degRadius = pi / 180.0
                in x * degRadius

calcGeoDistance :: (Double, Double) -> (Double, Double) -> Double
calcGeoDistance (long1, lat1) (long2, lat2) = let earthRadius = 6372797.560856
                                                  long1Radius = degToRadius long1
                                                  long2Radius = degToRadius long2
                                                  v           = sin ((long2Radius - long1Radius) / 2)
                                                  lat1Radius  = degToRadius lat1
                                                  lat2Radius  = degToRadius lat2
                                                  u           = sin ((lat2Radius - lat1Radius) / 2)
                                                  a           = u * u + cos lat1Radius * cos lat2Radius * v * v
                                                  in if v == 0
                                                     then abs (lat2Radius - lat1Radius) * earthRadius
                                                     else 2.0 * earthRadius * asin (sqrt a)

-------------------------------------------------------------------------------------------------

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

nowMS :: IO Integer
nowMS = fmap (floor . (* 1000)) getPOSIXTime

hasElapsedSince :: Integer -> Integer -> IO Bool
hasElapsedSince thresholdMs startNs = do
  endNs <- nowMS
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
