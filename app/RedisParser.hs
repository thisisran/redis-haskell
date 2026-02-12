module RedisParser
  ( parseOneCommand
  , parseSimpleString
  , parseRDBFile
  ) where

-- import Text.Read (readMaybe)

import Control.Monad (unless, void)
import Data.Char (toUpper)

import Data.Maybe (isNothing)

import Control.Applicative ((<|>))
import Data.Attoparsec.Combinator ((<?>))
import qualified Data.Attoparsec.ByteString as A
import qualified Data.Attoparsec.ByteString.Char8 as AC8

import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import Types

parseOneCommand :: BS.ByteString -> RParserResult
parseOneCommand input = case A.parse commandParser input of
                           A.Done rest cmd -> RParsed cmd rest
                           A.Partial _k    -> RParserNeedMore
                           A.Fail _ _ msg  -> RParserErr (BS8.pack msg)

parseSimpleString input = case A.parse simpleStringParser input of
                            A.Done rest str -> SParserFullString str rest
                            A.Partial _k    -> SParserPartialString
                            A.Fail _ _ msg  -> SParserError (BS8.pack msg)

parseRDBFile input = case A.parse rdbFileParser input of
                       A.Done rest content -> SParserFullString content rest
                       A.Partial _k        -> SParserPartialString
                       A.Fail _ _ msg      -> SParserError (BS8.pack msg)

simpleStringParser :: A.Parser BS.ByteString
simpleStringParser = do
  void $ AC8.char '+'
  A.takeTill (== 13) <* A.string "\r\n"     -- read until '\r' (not including it)

arrayLenParser :: A.Parser Int
arrayLenParser = do
  void $ AC8.char '*'
  AC8.decimal <* crlf

rdbFileParser :: A.Parser BS.ByteString
rdbFileParser = do
  void (AC8.char '$')
  n <- AC8.decimal <* crlf
  A.take n

bulkStringParser :: A.Parser BS.ByteString
bulkStringParser = do
  void $ AC8.char '$'
  n <- AC8.decimal <* crlf
  A.take n <* crlf

commandParser :: A.Parser Command
commandParser = do
  n <- arrayLenParser
  cmd <- fmap (BS8.map toUpper) bulkStringParser
  case cmd of
    "PING"     -> pingParser n
    "ECHO"     -> echoParser n
    "GET"      -> getParser n
    "SET"      -> setParser n
    "RPUSH"    -> rpushParser n
    "LPUSH"    -> lpushParser n
    "LRANGE"   -> lrangeParser n
    "LLEN"     -> llenParser n
    "LPOP"     -> lpopParser n
    "BLPOP"    -> blpopParser n
    "TYPE"     -> typeParser n
    "XADD"     -> xaddParser n
    "XRANGE"   -> xrangeParser n
    "XREAD"    -> xreadParser n
    "INCR"     -> incrParser n
    "MULTI"    -> multiParser n
    "EXEC"     -> execParser n
    "DISCARD"  -> discardParser n
    "INFO"     -> infoParser n
    "REPLCONF" -> replconfParser n
    "PSYNC"    -> psyncParser n
    "WAIT"     -> waitParser n
    "CONFIG"   -> configParser n
    "KEYS"     -> keysParser n
    "SUBSCRIBE" -> subscribeParser n
    "PUBLISH"   -> publishParser n
    "COMMAND"   -> cmdParser n -- convenience method, to allow redis-cli with no arguments
    "UNSUBSCRIBE" -> unsubcribeParser n
    "ZADD"        -> zaddParser n
    "ZRANK"       -> zrankParser n
    "ZRANGE"      -> zrangeParser n
    "ZCARD"       -> zcardParser n
    "ZSCORE"      -> zscoreParser n
    _           -> fail "unsupported command"
-- ===== command implementations =====
-- Note: n counts *all* array elements including the command name itself.

zscoreParser :: Int -> A.Parser Command
zscoreParser n = do
  expectArity [3] n
  ZScore <$> bulkStringParser <*> bulkStringParser

zcardParser :: Int -> A.Parser Command
zcardParser n = do
  expectArity [2] n
  ZCard <$> bulkStringParser

zrangeParser :: Int -> A.Parser Command
zrangeParser n = do
  expectArity [4] n
  ZRange <$> bulkStringParser <*> (bulkStringStartParser >> AC8.signed AC8.decimal <* crlf) <*> (bulkStringStartParser >> AC8.signed AC8.decimal <* crlf)

zrankParser :: Int -> A.Parser Command
zrankParser n = do
  expectArity [3] n
  ZRank <$> bulkStringParser <*> bulkStringParser

zaddParser :: Int -> A.Parser Command
zaddParser n = do
  expectArity [4] n
  ZAdd <$> bulkStringParser <*> (bulkStringStartParser >> AC8.double <* crlf) <*> bulkStringParser

unsubcribeParser :: Int -> A.Parser Command
unsubcribeParser n = do
  expectArity [2] n
  Unsubscribe <$> bulkStringParser

cmdParser :: Int -> A.Parser Command
cmdParser n = do
  expectArity [2] n
  bulkStringStartParser >> AC8.stringCI "docs" >> crlf
  pure Cmd
  
publishParser :: Int -> A.Parser Command
publishParser n = do
  expectArity [3] n
  Publish <$> bulkStringParser <*> bulkStringParser

subscribeParser :: Int -> A.Parser Command
subscribeParser n = do
  expectArity [2] n
  Subscribe <$> bulkStringParser

keysParser :: Int -> A.Parser Command
keysParser n = do
  expectArity [2] n
  Keys <$> bulkStringParser

configParser :: Int  -> A.Parser Command
configParser n = do
  expectArity [3] n
  parseDir <|> parseFileName
  where parseDir = do
          bulkStringStartParser >> AC8.stringCI "GET" >> crlf
          bulkStringStartParser >> AC8.stringCI "dir" >> crlf
          pure $ Config ConfigGetDir
        parseFileName = do
          bulkStringStartParser >> AC8.stringCI "GET" >> crlf
          bulkStringStartParser >> AC8.stringCI "dbfilename" >> crlf
          pure $ Config ConfigGetFileName

-- *3\r\n$4\r\nWAIT\r\n$1\r\n0\r\n$5\r\n60000\r\n
waitParser :: Int -> A.Parser Command
waitParser n = do
  expectArity [3] n
  void bulkStringStartParser
  n <- AC8.decimal <* crlf
  timeout <- bulkStringStartParser *> AC8.double <* crlf
  pure $ Wait n timeout

pingParser :: Int -> A.Parser Command
pingParser n = do
  expectArity [1] n
  pure Ping

echoParser :: Int -> A.Parser Command
echoParser n = do
  expectArity [2] n
  Echo <$> bulkStringParser

getParser :: Int -> A.Parser Command
getParser n = do
  expectArity [2] n
  Get <$> bulkStringParser

expiryParser :: A.Parser SetExpiry
expiryParser = do
  opt <- fmap (BS8.map toUpper) bulkStringParser
  numBs <- bulkStringParser
  num <- case BS8.readInt numBs of
    Just (x, rest) | BS.null rest -> pure x
    _ -> fail "expected integer for expiry setting"
  case opt of
    "EX" -> pure (EX num)
    "PX" -> pure (PX num)
    _    -> fail "expected EX or PX for expiry setting"

setParser :: Int -> A.Parser Command
setParser n = do
  expectArity [3,5] n
  key <- bulkStringParser
  val <- bulkStringParser
  expiry <- if n == 5 then Just <$> expiryParser else pure Nothing
  pure (Set key val expiry)

countBulkStringParser :: Int -> A.Parser [BS.ByteString]
countBulkStringParser k
  | k <= 0    = pure []
  | otherwise = sequenceA (replicate k bulkStringParser)

pushParser :: Int -> A.Parser (BS.ByteString, [BS.ByteString])
pushParser n = do
  expectMinArity 3 n
  key    <- bulkStringParser
  values <- countBulkStringParser (n - 2)
  pure (key, values)
  
rpushParser :: Int -> A.Parser Command
rpushParser n = do
  (key, values) <- pushParser n
  pure (RPush key values)

lpushParser :: Int -> A.Parser Command
lpushParser n = do
  (key, values) <- pushParser n
  pure (LPush key values)

bulkAs :: A.Parser a -> A.Parser a
bulkAs p = do
  bs <- bulkStringParser
  case A.parseOnly (p <* A.endOfInput) bs of
    Left err -> fail err
    Right x  -> pure x

signedIntBulk :: A.Parser Int
signedIntBulk = bulkAs (AC8.signed AC8.decimal)

lrangeParser :: Int -> A.Parser Command
lrangeParser n = do
  expectArity [4] n
  LRange <$> bulkStringParser <*> signedIntBulk <*> signedIntBulk

llenParser :: Int -> A.Parser Command
llenParser n = do
  expectArity [2] n
  LLen <$> bulkStringParser

lpopParser :: Int -> A.Parser Command
lpopParser n = do
  expectArity [2, 3] n
  key <- bulkStringParser
  count <- if n == 3
           then  do
             countBs <- bulkStringParser
             case BS8.readInt countBs of
               Just (x, rest) | BS.null rest -> pure x
               _ -> fail "expected integer (count) for lpop"
           else pure 1
  pure (LPop key count)

blpopParser :: Int -> A.Parser Command
blpopParser n = do
  expectArity [3] n
  key <- bulkStringParser <* bulkStringStartParser
  timeout <- AC8.double <* crlf
  pure $ BLPop key timeout

typeParser :: Int -> A.Parser Command
typeParser n = do
  expectArity [2] n
  Type <$> bulkStringParser

crlf :: A.Parser BS.ByteString
crlf = A.string "\r\n"

bulkStringStartParser :: A.Parser BS.ByteString
bulkStringStartParser = do
  void (AC8.char '$')
  void AC8.decimal
  crlf

entryIdParser :: A.Parser EntryId
entryIdParser = do
  void bulkStringStartParser
  parseAuto <|> (parseMili >>= (\pre -> fullSeq pre <|> missingSeq pre))
  where
    parseMili = do
      pre <- AC8.decimal
      void (AC8.char '-')
      pure pre
    parseAuto = do
      void (AC8.char '*') <* crlf
      pure EntryGenNew
    fullSeq p = do
      post <- AC8.decimal <* crlf
      pure (EntryId p post)
    missingSeq p = do
      void (AC8.char '*') <* crlf
      pure (EntryGenSeq p)

countKeyValueParser :: Int -> A.Parser [(BS.ByteString, BS.ByteString)]
countKeyValueParser k
  | k <= 0    = pure []
  | otherwise = sequenceA (replicate k (do
    key <- bulkStringParser
    value <- bulkStringParser
    pure (key, value)))

xaddParser :: Int -> A.Parser Command
xaddParser n = do
  expectMinArity 5 n
  streamID <- bulkStringParser
  entryID <- entryIdParser
  values <- countKeyValueParser $ (n - 3) `div` 2
  pure (XAdd streamID entryID values)

fullRangeParser :: A.Parser RangeEntryId
fullRangeParser = do
  void bulkStringStartParser
  mili <- AC8.decimal <* AC8.char '-'
  seq <- AC8.decimal <* crlf
  pure $ RangeEntryId mili seq

miliRangeParser :: A.Parser RangeEntryId
miliRangeParser = do
  void bulkStringStartParser
  mili <- AC8.decimal <* crlf
  pure $ RangeMili mili

parseMinusMiliRange :: A.Parser RangeEntryId
parseMinusMiliRange = do
  void bulkStringStartParser
  void (AC8.char '-') <* crlf
  pure RangeMinusPlus

plusSeqRangeParser :: A.Parser RangeEntryId
plusSeqRangeParser = do
  void bulkStringStartParser <* AC8.char '+' <* crlf
  pure RangeMinusPlus

dollarSignParser :: A.Parser RangeEntryId
dollarSignParser = do
  void bulkStringStartParser
  void (AC8.char '$') <* crlf
  pure RangeDollar

endRangeParser :: A.Parser RangeEntryId
endRangeParser = fullRangeParser <|> miliRangeParser <|> plusSeqRangeParser

startRangeParser :: A.Parser RangeEntryId
startRangeParser = fullRangeParser <|> miliRangeParser <|> parseMinusMiliRange <|> dollarSignParser

xrangeParser :: Int -> A.Parser Command
xrangeParser n = do
  expectMinArity 4 n
  XRange <$> bulkStringParser <*> startRangeParser <*> endRangeParser

blockKeywordParser :: A.Parser (Maybe Double)
blockKeywordParser = do
   (void bulkStringStartParser >> (AC8.stringCI "block" >> crlf >> void bulkStringStartParser >> (Just <$> AC8.double) <* crlf)) <|> pure Nothing

countStartRange :: Int -> A.Parser [RangeEntryId]
countStartRange k
  | k <= 0    = pure []
  | otherwise = sequenceA (replicate k startRangeParser)

xreadParser :: Int -> A.Parser Command
xreadParser n = do
  expectMinArity 4 n
  timeout <- blockKeywordParser
  let count = if isNothing timeout then 2 else 4
  void bulkStringStartParser *> AC8.stringCI "streams" <* crlf
  keys <- countBulkStringParser $ (n - count) `div` 2
  ids <- countStartRange $ (n - count) `div` 2
  pure (XRead (zip keys ids) timeout)

incrParser :: Int -> A.Parser Command
incrParser n = do
  expectArity [2] n
  Incr <$> bulkStringParser

multiParser :: Int -> A.Parser Command
multiParser n = do
  expectArity [1] n
  pure Multi

execParser :: Int -> A.Parser Command
execParser n = do
  expectArity [1] n
  pure Exec

discardParser :: Int -> A.Parser Command
discardParser n = do
  expectArity [1] n
  pure Discard

infoParser :: Int -> A.Parser Command
infoParser n = do
  expectMinArity 1 n
  if n == 1
  then pure $ Info FullInfo
  else do
        parseReplication <|> parseServer <|> parseClients <|> parseMemory
        where
           parseReplication = void bulkStringStartParser >> AC8.stringCI "replication" >> crlf >> pure (Info Replication)
           parseServer = void bulkStringStartParser >> AC8.stringCI "server" >> crlf >> pure (Info Server)
           parseClients = void bulkStringStartParser >> AC8.stringCI "clients" >> crlf >> pure (Info Clients)
           parseMemory = void bulkStringStartParser >> AC8.stringCI "memory" >> crlf >> pure (Info Memory)

-- n = 3, *3\r\n
-- $8\r\nREPLCONF\r\n
-- $3\r\nACK\r\n
-- $2\r\n31\r\n

replconfParser :: Int  -> A.Parser Command
replconfParser n = do
  expectArity [3] n
  listeningPortParser <|> capabilityParser <|> getackCheckParser <|> getackRespParser
  where listeningPortParser = do
          void bulkStringStartParser *> AC8.stringCI "listening-port" <* crlf *> void bulkStringStartParser
          port <- (A.takeWhile1 isDigitW8 <* crlf) <?> "parsing listening port"
          pure $ ReplConf (ListeningPort port)
        isDigitW8 w = w >= 48 && w <= 57
        capabilityParser = do
          void bulkStringStartParser <* AC8.stringCI "capa" <* crlf
          ReplConf . Capa <$> bulkStringParser
        getackCheckParser = do
          void bulkStringStartParser <* AC8.stringCI "GETACK" <* crlf
          void bulkStringStartParser <* AC8.char '*' <* crlf
          pure $ ReplConf GetAck
        getackRespParser = do
          bulkStringStartParser *> void (AC8.stringCI "ACK") <* crlf
          void bulkStringStartParser
          offset <- AC8.decimal <* crlf
          pure $ ReplConf (AckWith offset)

psyncParser :: Int -> A.Parser Command
psyncParser n = do
  expectArity [3] n
  unknownParser <|> fullParser
      where unknownParser = do
              bulkStringStartParser <* AC8.char '?' <* crlf
              pure $ Psync PSyncUnknown
            fullParser = do
              replID <- bulkStringParser
              bulkStringStartParser
              replOffset <- AC8.decimal <* crlf
              pure $ Psync (PSyncFull replID replOffset)

expectArity :: [Int] -> Int -> A.Parser ()
expectArity allowed n =
  unless (n `elem` allowed) $
    fail ("wrong arity: expected " ++ show allowed ++ ", got " ++ show n)

expectMinArity :: Int -> Int -> A.Parser ()
expectMinArity minN n =
  unless (n >= minN) $
    fail ("wrong arity (expected >= " ++ show minN ++ ", got " ++ show n ++ ")")
