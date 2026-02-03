{-# LANGUAGE OverloadedStrings #-}

module Parser
  ( parseCommand
    , Command (..)
    , PushCommand (..)
    , SetExpiry (..)
    , runParse
    , prettifyErrors
  ) where

import Text.Read (readMaybe)

import Control.Monad (unless, void)
import Data.Char (toUpper)
import Data.Function (on)
import Data.List (find)
import Data.Maybe (fromMaybe)
import Data.Void (Void)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import Text.Megaparsec (Parsec, takeP, (<?>), eof, parse, errorBundlePretty)
import Text.Megaparsec.Stream (VisualStream, TraversableStream)
import Text.Megaparsec.Error (ShowErrorComponent, ParseErrorBundle)

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
  ]

lookupSpec :: BS.ByteString -> [CommandSpec] -> Maybe CommandSpec
lookupSpec cmd = find (\s -> any (ciEq cmd) (names s))

ciEq :: BS.ByteString -> BS.ByteString -> Bool
ciEq = (==) `on` (BSC.map toUpper)

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
  entryID <- parseStreamId
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

parseStreamId :: Parser MS.EntryId
parseStreamId = do
  void (B.char 36) -- '$'
  L.decimal
  B.crlf
  pre <- L.decimal
  B.char 45
  post <- L.decimal
  B.crlf
  pure (MS.EntryId pre post)
