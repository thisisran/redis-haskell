{-# LANGUAGE OverloadedStrings #-}

module Parser
  ( parseCommand
    , Command (..)
    , PushCommand (..)
    , SetExpiry (..)
    , runParse
    , prettifyErrors
    -- , parseEntryId
  ) where

import Text.Read (readMaybe)

import Control.Monad (unless, void)
import Data.Char (toUpper)
import Data.Function (on)
import Data.List (find)
import Data.Maybe (fromMaybe, isNothing)
import Data.Void (Void)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import Text.Megaparsec (Parsec, takeP, (<?>), parse, errorBundlePretty, (<|>), try)
import Text.Megaparsec.Stream (VisualStream, TraversableStream)
import Text.Megaparsec.Error (ShowErrorComponent, ParseErrorBundle)
import Text.Megaparsec.Char (string)

import qualified Text.Megaparsec.Byte as B
import qualified Text.Megaparsec as M
import qualified Text.Megaparsec.Byte.Lexer as L

import qualified MemoryStore as MS

type Parser = Parsec Void BS.ByteString

data SetExpiry = EX Int | PX Int
  deriving (Show, Eq)

data PushCommand = RightPushCmd | LeftPushCmd
  deriving (Show, Eq)

data Command
  = Ping
  | Echo BS.ByteString
  | Set BS.ByteString BS.ByteString (Maybe SetExpiry)
  | Get BS.ByteString
  | RPush BS.ByteString [BS.ByteString]
  | LPush BS.ByteString [BS.ByteString]
  | LRange BS.ByteString Int Int
  | LLen BS.ByteString
  | LPop BS.ByteString Int
  | BLPop BS.ByteString Double
  | Type BS.ByteString
  | XAdd BS.ByteString MS.EntryId [(BS.ByteString, BS.ByteString)]
  | XRange BS.ByteString MS.RangeEntryId MS.RangeEntryId
  | XRead [(BS.ByteString, MS.RangeEntryId)] (Maybe Double)
  deriving (Show, Eq)

parseBulkString :: Parser BS.ByteString
parseBulkString = do
  void (B.char 36) -- '$'
  n <- L.decimal
  B.crlf
  takeP (Just "bulk string") n <* B.crlf

parseArrayLen :: Parser Int
parseArrayLen = do
  void (B.char 42) -- '*'
  n <- L.decimal
  B.crlf
  pure n

data CommandSpec = CommandSpec
  { names :: [BS.ByteString]         -- command name(s)
  , run   :: Int -> Parser Command   -- parses remaining bulk args (command name already consumed)
  }

parseCommand :: Parser Command
parseCommand = do
  n   <- parseArrayLen
  cmd <- parseBulkString

  spec <- maybe (fail "unsupported command") pure (lookupSpec cmd specs)
  run spec n <?> "command args"

specs :: [CommandSpec]
specs =
  [ CommandSpec ["PING"] parsePING
  , CommandSpec ["ECHO"] parseECHO
  , CommandSpec ["GET"] parseGET
  , CommandSpec ["SET"] parseSET
  , CommandSpec ["RPUSH"] parseRPUSH
  , CommandSpec ["LPUSH"] parseLPUSH
  , CommandSpec ["LRANGE"] parseLRANGE
  , CommandSpec ["LLEN"] parseLLEN
  , CommandSpec ["LPOP"] parseLPOP
  , CommandSpec ["BLPOP"] parseBLPOP
  , CommandSpec ["TYPE"] parseTYPE
  , CommandSpec ["XADD"] parseXADD
  , CommandSpec ["XRANGE"] parseXRANGE
  , CommandSpec ["XREAD"] parseXREAD
  ]

lookupSpec :: BS.ByteString -> [CommandSpec] -> Maybe CommandSpec
lookupSpec cmd = find (\s -> any (ciEq cmd) (names s))

ciEq :: BS.ByteString -> BS.ByteString -> Bool
ciEq = (==) `on` BSC.map toUpper

expectArity :: [Int] -> Int -> Parser ()
expectArity allowed n =
  unless (n `elem` allowed) $
    fail ("wrong arity (expected " ++ show allowed ++ ", got " ++ show n ++ ")")

expectMinArity :: Int -> Int -> Parser ()
expectMinArity minN n =
  unless (n >= minN) $
    fail ("wrong arity (expected >= " ++ show minN ++ ", got " ++ show n ++ ")")

readIntBS :: BS.ByteString -> Parser Int
readIntBS bs = case BSC.readInt bs of
  Just (n, rest) | BSC.null rest -> pure n
  _ -> fail "expected integer"

readDoubleBS :: BS.ByteString -> Parser Double
readDoubleBS bs = case (readMaybe . BSC.unpack) bs of
  Just n -> pure n
  _ -> fail "expected double/integer"

-- ===== command implementations =====
-- Note: n counts *all* array elements including the command name itself.

parsePING :: Int -> Parser Command
parsePING n = do
  expectArity [1] n
  pure Ping

parseECHO :: Int -> Parser Command
parseECHO n = do
  expectArity [2] n
  Echo <$> parseBulkString                   

parseGET :: Int -> Parser Command
parseGET n = do
  expectArity [2] n
  Get <$> parseBulkString

parseSET :: Int -> Parser Command
parseSET n = do
  expectArity [3,5] n
  key <- parseBulkString
  val <- parseBulkString
  expiry <- if n == 5 then Just <$> parseExpiry else pure Nothing
  pure (Set key val expiry)

parseExpiry :: Parser SetExpiry
parseExpiry = do
  opt <- parseBulkString
  num <- readIntBS =<< parseBulkString
  if opt `ciEq` "EX" then pure (EX num)
  else if opt `ciEq` "PX" then pure (PX num)
         else fail "expected EX or PX"

parseRPUSH :: Int -> Parser Command
parseRPUSH n = do
  expectMinArity 3 n
  key    <- parseBulkString
  values <- countBulk (n - 2)
  pure (RPush key values)

parseLPUSH :: Int -> Parser Command
parseLPUSH n = do
  expectMinArity 3 n
  key    <- parseBulkString
  values <- countBulk (n - 2)
  pure (LPush key values)

countBulk :: Int -> Parser [BS.ByteString]
countBulk k
  | k <= 0    = pure []
  | otherwise = sequenceA (replicate k parseBulkString)

parseLRANGE :: Int -> Parser Command
parseLRANGE n = do
  expectArity [4] n
  key   <- parseBulkString
  start <- readIntBS =<< parseBulkString
  stop  <- readIntBS =<< parseBulkString
  pure (LRange key start stop)

parseLLEN :: Int -> Parser Command
parseLLEN n = do
  expectArity [2] n
  LLen <$> parseBulkString

parseLPOP :: Int -> Parser Command
parseLPOP n = do
  expectArity [2, 3] n
  key <- parseBulkString
  count <- if n == 3 then readIntBS =<< parseBulkString else pure 1
  pure (LPop key count)

parseBLPOP :: Int -> Parser Command
parseBLPOP n = do
  expectArity [3] n
  key <- parseBulkString
  timeout <- readDoubleBS =<< parseBulkString
  pure (BLPop key timeout)

parseTYPE :: Int -> Parser Command
parseTYPE n = do
  expectArity [2] n
  Type <$> parseBulkString

parseXADD :: Int -> Parser Command
parseXADD n = do
  expectMinArity 5 n
  streamID <- parseBulkString
  entryID <- parseEntryId
  values <- countKeyValue $ (n - 3) `div` 2
  pure (XAdd streamID entryID values)

countKeyValue :: Int -> Parser [(BS.ByteString, BS.ByteString)]
countKeyValue k
  | k <= 0    = pure []
  | otherwise = sequenceA (replicate k (do
    key <- parseBulkString
    value <- parseBulkString
    pure (key, value)))

runParse = parse parseCommand "<input>"

prettifyErrors :: (Text.Megaparsec.Stream.VisualStream s,
      Text.Megaparsec.Stream.TraversableStream s,
      Text.Megaparsec.Error.ShowErrorComponent e) =>
     Text.Megaparsec.Error.ParseErrorBundle s e -> String
prettifyErrors = errorBundlePretty

parseEntryId :: Parser MS.EntryId
parseEntryId = do
  void parseBulkStringStart
  parseAuto <|> (parseMili >>= (\pre -> fullSeq pre <|> missingSeq pre))
  where
    parseAuto = do
      void (B.char 42)
      B.crlf
      pure MS.EntryGenNew
    parseMili = do
      pre <- L.decimal
      void (B.char 45) -- '-'
      pure pre
    fullSeq p = do
      post <- L.decimal
      B.crlf
      pure (MS.EntryId p post)
    missingSeq p = do
      void (B.char 42) -- *
      B.crlf
      pure (MS.EntryGenSeq p)

parseFullRange :: Parser MS.RangeEntryId
parseFullRange = do
  void parseBulkStringStart
  mili <- L.decimal
  void (B.char 45)
  seq <- L.decimal
  B.crlf
  pure $ MS.RangeEntryId mili seq

parseMiliRange :: Parser MS.RangeEntryId
parseMiliRange = do
  void parseBulkStringStart
  mili <- L.decimal
  B.crlf
  pure $ MS.RangeMili mili

parseMinusMiliRange :: Parser MS.RangeEntryId
parseMinusMiliRange = do
  void parseBulkStringStart
  void (B.char 45)
  B.crlf
  pure MS.RangeMinusPlus

parsePlusSeqRange :: Parser MS.RangeEntryId
parsePlusSeqRange = do
  void parseBulkStringStart
  void (B.char 43)
  B.crlf
  pure MS.RangeMinusPlus

parseDollarSign :: Parser MS.RangeEntryId
parseDollarSign = do
  void parseBulkStringStart
  void (B.char 36)
  B.crlf
  pure MS.RangeDollar

parseStartRange :: Parser MS.RangeEntryId
parseStartRange = try (try parseFullRange <|> try parseMiliRange <|> parseMinusMiliRange) <|> parseDollarSign

parseEndRange :: Parser MS.RangeEntryId
parseEndRange = try parseFullRange <|> try parseMiliRange <|> parsePlusSeqRange

parseXRANGE :: Int -> Parser Command
parseXRANGE n = do
  expectMinArity 4 n
  XRange <$> parseBulkString <*> parseStartRange <*> parseEndRange

countStartRange :: Int -> Parser [MS.RangeEntryId]
countStartRange k
  | k <= 0    = pure []
  | otherwise = sequenceA (replicate k parseStartRange)

parseBulkStringStart :: Parser BS.ByteString
parseBulkStringStart = do
  void (B.char 36) -- '$'
  L.decimal
  B.crlf

parseBlockKeyword :: Parser (Maybe Double)
parseBlockKeyword = do
  try (void parseBulkStringStart >> (B.string' "block" >> B.crlf >> void parseBulkStringStart >> (Just <$> L.decimal) <* B.crlf)) <|> pure Nothing

parseXREAD :: Int -> Parser Command
parseXREAD n = do
  expectMinArity 4 n
  timeout <- parseBlockKeyword
  let count = if isNothing timeout then 2 else 4
  void parseBulkStringStart
  B.string' "streams"
  B.crlf
  keys <- countBulk $ (n - count) `div` 2
  ids <- countStartRange $ (n - count) `div` 2
  pure (XRead (zip keys ids) timeout)
