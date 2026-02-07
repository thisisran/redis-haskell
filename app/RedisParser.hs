{-# LANGUAGE OverloadedStrings #-}

module RedisParser
  ( parseCommand
  , runParse
  , prettifyErrors
  ) where

import Text.Read (readMaybe)

import Control.Monad (unless, void)
import Data.Char (toUpper)
import Data.Function (on)
import Data.List (find)
import Data.Maybe (isNothing)
import Text.Megaparsec (takeP, (<?>), parse, errorBundlePretty, (<|>), try)
import Text.Megaparsec.Stream (VisualStream, TraversableStream)
import Text.Megaparsec.Error (ShowErrorComponent, ParseErrorBundle)
import Text.Megaparsec.Char (string)

import qualified Text.Megaparsec as M
import qualified Text.Megaparsec.Byte as B
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8
import qualified Text.Megaparsec.Byte.Lexer as L

import Types

parseBulkString :: RedisParser BS.ByteString
parseBulkString = do
  void (B.char 36) -- '$'
  n <- L.decimal
  B.crlf
  takeP (Just "bulk string") n <* B.crlf

parseArrayLen :: RedisParser Int
parseArrayLen = do
  void (B.char 42) -- '*'
  n <- L.decimal
  B.crlf
  pure n

data CommandSpec = CommandSpec
  { names :: [BS.ByteString]         -- command name(s)
  , run   :: Int -> RedisParser Command   -- parses remaining bulk args (command name already consumed)
  }

parseCommand :: RedisParser Command
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
  , CommandSpec ["INCR"] parseINCR
  , CommandSpec ["MULTI"] parseMULTI
  , CommandSpec ["EXEC"] parseEXEC
  , CommandSpec ["DISCARD"] parseDISCARD
  , CommandSpec ["INFO"] parseINFO
  ]

lookupSpec :: BS.ByteString -> [CommandSpec] -> Maybe CommandSpec
lookupSpec cmd = find (\s -> any (ciEq cmd) (names s))

ciEq :: BS.ByteString -> BS.ByteString -> Bool
ciEq = (==) `on` BS8.map toUpper

expectArity :: [Int] -> Int -> RedisParser ()
expectArity allowed n =
  unless (n `elem` allowed) $
    fail ("wrong arity (expected " ++ show allowed ++ ", got " ++ show n ++ ")")

expectMinArity :: Int -> Int -> RedisParser ()
expectMinArity minN n =
  unless (n >= minN) $
    fail ("wrong arity (expected >= " ++ show minN ++ ", got " ++ show n ++ ")")

readIntBS :: BS.ByteString -> RedisParser Int
readIntBS bs = case BS8.readInt bs of
  Just (n, rest) | BS8.null rest -> pure n
  _ -> fail "expected integer"

readDoubleBS :: BS.ByteString -> RedisParser Double
readDoubleBS bs = case (readMaybe . BS8.unpack) bs of
  Just n -> pure n
  _ -> fail "expected double/integer"

-- ===== command implementations =====
-- Note: n counts *all* array elements including the command name itself.

parsePING :: Int -> RedisParser Command
parsePING n = do
  expectArity [1] n
  pure Ping

parseECHO :: Int -> RedisParser Command
parseECHO n = do
  expectArity [2] n
  Echo <$> parseBulkString                   

parseGET :: Int -> RedisParser Command
parseGET n = do
  expectArity [2] n
  Get <$> parseBulkString

parseSET :: Int -> RedisParser Command
parseSET n = do
  expectArity [3,5] n
  key <- parseBulkString
  val <- parseBulkString
  expiry <- if n == 5 then Just <$> parseExpiry else pure Nothing
  pure (Set key val expiry)

parseExpiry :: RedisParser SetExpiry
parseExpiry = do
  opt <- parseBulkString
  num <- readIntBS =<< parseBulkString
  if opt `ciEq` "EX" then pure (EX num)
  else if opt `ciEq` "PX" then pure (PX num)
         else fail "expected EX or PX"

parseRPUSH :: Int -> RedisParser Command
parseRPUSH n = do
  expectMinArity 3 n
  key    <- parseBulkString
  values <- countBulk (n - 2)
  pure (RPush key values)

parseLPUSH :: Int -> RedisParser Command
parseLPUSH n = do
  expectMinArity 3 n
  key    <- parseBulkString
  values <- countBulk (n - 2)
  pure (LPush key values)

countBulk :: Int -> RedisParser [BS.ByteString]
countBulk k
  | k <= 0    = pure []
  | otherwise = sequenceA (replicate k parseBulkString)

parseLRANGE :: Int -> RedisParser Command
parseLRANGE n = do
  expectArity [4] n
  key   <- parseBulkString
  start <- readIntBS =<< parseBulkString
  stop  <- readIntBS =<< parseBulkString
  pure (LRange key start stop)

parseLLEN :: Int -> RedisParser Command
parseLLEN n = do
  expectArity [2] n
  LLen <$> parseBulkString

parseLPOP :: Int -> RedisParser Command
parseLPOP n = do
  expectArity [2, 3] n
  key <- parseBulkString
  count <- if n == 3 then readIntBS =<< parseBulkString else pure 1
  pure (LPop key count)

parseBLPOP :: Int -> RedisParser Command
parseBLPOP n = do
  expectArity [3] n
  key <- parseBulkString
  timeout <- readDoubleBS =<< parseBulkString
  pure (BLPop key timeout)

parseTYPE :: Int -> RedisParser Command
parseTYPE n = do
  expectArity [2] n
  Type <$> parseBulkString

parseXADD :: Int -> RedisParser Command
parseXADD n = do
  expectMinArity 5 n
  streamID <- parseBulkString
  entryID <- parseEntryId
  values <- countKeyValue $ (n - 3) `div` 2
  pure (XAdd streamID entryID values)

countKeyValue :: Int -> RedisParser [(BS.ByteString, BS.ByteString)]
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

parseEntryId :: RedisParser EntryId
parseEntryId = do
  void parseBulkStringStart
  parseAuto <|> (parseMili >>= (\pre -> fullSeq pre <|> missingSeq pre))
  where
    parseAuto = do
      void (B.char 42)
      B.crlf
      pure EntryGenNew
    parseMili = do
      pre <- L.decimal
      void (B.char 45) -- '-'
      pure pre
    fullSeq p = do
      post <- L.decimal
      B.crlf
      pure (EntryId p post)
    missingSeq p = do
      void (B.char 42) -- *
      B.crlf
      pure (EntryGenSeq p)

parseFullRange :: RedisParser RangeEntryId
parseFullRange = do
  void parseBulkStringStart
  mili <- L.decimal
  void (B.char 45)
  seq <- L.decimal
  B.crlf
  pure $ RangeEntryId mili seq

parseMiliRange :: RedisParser RangeEntryId
parseMiliRange = do
  void parseBulkStringStart
  mili <- L.decimal
  B.crlf
  pure $ RangeMili mili

parseMinusMiliRange :: RedisParser RangeEntryId
parseMinusMiliRange = do
  void parseBulkStringStart
  void (B.char 45)
  B.crlf
  pure RangeMinusPlus

parsePlusSeqRange :: RedisParser RangeEntryId
parsePlusSeqRange = do
  void parseBulkStringStart
  void (B.char 43)
  B.crlf
  pure RangeMinusPlus

parseDollarSign :: RedisParser RangeEntryId
parseDollarSign = do
  void parseBulkStringStart
  void (B.char 36)
  B.crlf
  pure RangeDollar

parseStartRange :: RedisParser RangeEntryId
parseStartRange = try (try parseFullRange <|> try parseMiliRange <|> parseMinusMiliRange) <|> parseDollarSign

parseEndRange :: RedisParser RangeEntryId
parseEndRange = try parseFullRange <|> try parseMiliRange <|> parsePlusSeqRange

parseXRANGE :: Int -> RedisParser Command
parseXRANGE n = do
  expectMinArity 4 n
  XRange <$> parseBulkString <*> parseStartRange <*> parseEndRange

countStartRange :: Int -> RedisParser [RangeEntryId]
countStartRange k
  | k <= 0    = pure []
  | otherwise = sequenceA (replicate k parseStartRange)

parseBulkStringStart :: RedisParser BS.ByteString
parseBulkStringStart = do
  void (B.char 36) -- '$'
  L.decimal
  B.crlf

parseBlockKeyword :: RedisParser (Maybe Double)
parseBlockKeyword = do
  try (void parseBulkStringStart >> (B.string' "block" >> B.crlf >> void parseBulkStringStart >> (Just <$> L.decimal) <* B.crlf)) <|> pure Nothing

parseXREAD :: Int -> RedisParser Command
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

parseINCR :: Int -> RedisParser Command
parseINCR n = do
  expectArity [2] n
  Incr <$> parseBulkString

parseMULTI :: Int -> RedisParser Command
parseMULTI n = do
  expectArity [1] n
  pure Multi

parseEXEC :: Int -> RedisParser Command
parseEXEC n = do
  expectArity [1] n
  pure Exec

parseDISCARD :: Int -> RedisParser Command
parseDISCARD n = do
  expectArity [1] n
  pure Discard 

parseINFO :: Int -> RedisParser Command
parseINFO n = do
  expectMinArity 1 n
  if n == 1
  then pure $ Info FullInfo
  else do
      try parseReplication <|> try parseServer <|> try parseClients <|> parseMemory
  where parseReplication = void parseBulkStringStart >> B.string' "replication" >> B.crlf >> pure (Info Replication)
        parseServer = void parseBulkStringStart >> B.string' "server" >> B.crlf >> pure (Info Server)
        parseClients = void parseBulkStringStart >> B.string' "clients" >> B.crlf >> pure (Info Clients)
        parseMemory = void parseBulkStringStart >> B.string' "memory" >> B.crlf >> pure (Info Memory)
