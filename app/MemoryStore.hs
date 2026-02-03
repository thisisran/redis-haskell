module MemoryStore
  ( MemoryStoreEntry (..)
  , MemoryStore
  , MemoryStoreValue (..)
  , BLPopWaiter (..)
  , newMemoryStore
  , getMemoryDataVal
  , setMemoryDataKey
  , delMemoryDataKey
  , getMemoryWaitersVal
  , setMemoryWaitersKey
  , delMemoryWaitersKey
  , addMemoryWaiter
  , entryIdToBS
  , EntryId (..)
  , Stream (..)
  , Streams (..)
  ) where

import Control.Concurrent.STM (atomically, TVar, readTVar, writeTVar, newTVarIO, readTVarIO, modifyTVar')

import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M

import Control.Monad (unless)

import qualified Types as T

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Char8 as BS8

import Data.Word (Word64)

data EntryId = EntryId !Word64 !Word64
  deriving (Eq, Ord, Show)

entryIdToBS :: EntryId -> BS.ByteString
entryIdToBS (EntryId ms seq) =
  BSL.toStrict $
    BB.toLazyByteString $
      BB.word64Dec ms <> BB.char7 '-' <> BB.word64Dec seq

parseStreamId :: BS.ByteString -> Maybe EntryId
parseStreamId bs = do
  let (a, rest) = BS8.break (=='-') bs
  ('-', b) <- BS8.uncons rest
  (msI, r1)  <- BS8.readInteger a
  (seqI, r2) <- BS8.readInteger b
  if BS8.null r1 && BS8.null r2 && msI >= 0 && seqI >= 0
    then do
      ms  <- toWord64 msI
      seq <- toWord64 seqI
      pure (EntryId ms seq)
    else Nothing
  where
    toWord64 i
      | i <= fromIntegral (maxBound :: Word64) = Just (fromIntegral i)
      | otherwise                              = Nothing

newtype Stream a    = Stream (M.Map EntryId a)
                      deriving (Eq, Show)
newtype Streams n a = Streams (HM.HashMap n (Stream a))
                      deriving (Eq, Show)

readAfter :: EntryId -> Stream a -> [(EntryId, a)]
readAfter sid (Stream stream) = (M.toAscList . snd . M.split sid) stream

readFromInclusive :: EntryId -> Stream a -> [(EntryId, a)]
readFromInclusive sid (Stream m) =
  case M.splitLookup sid m of
    (_lt, Nothing, gt) -> M.toAscList gt
    (_lt, Just v, gt)  -> (sid, v) : M.toAscList gt

-- limit count (like XREAD COUNT)
readAfterN :: Int -> EntryId -> Stream a -> [(EntryId, a)]
readAfterN n sid = take n . readAfter sid

data MemoryStoreValue = MSStringVal BS.ByteString
                      | MSListVal [BS.ByteString]
                      | MSStreams (Streams BS.ByteString (M.Map BS.ByteString BS.ByteString))
                      deriving (Eq, Show)

newtype BLPopWaiter = BLPopWaiter T.ClientID deriving (Eq, Show)
type BLPopWaiters = [BLPopWaiter]

data MemoryStoreEntry = MemoryStoreEntry
  { val :: MemoryStoreValue,
    expiresAt :: Maybe (T.ExpireDuration, T.ExpireReference)
  } deriving (Eq, Show)

data MemoryStore = MemoryStore
  { msData :: TVar (M.Map BS.ByteString MemoryStoreEntry)
  , msBLPopWaiters :: TVar (M.Map BS.ByteString BLPopWaiters)
  }

newMemoryStore :: IO MemoryStore
newMemoryStore = MemoryStore <$> newTVarIO M.empty <*> newTVarIO M.empty

getMemoryDataVal :: MemoryStore -> T.DataKey -> IO (Maybe MemoryStoreEntry)
getMemoryDataVal (MemoryStore d w) k = M.lookup k <$> readTVarIO d

setMemoryDataKey :: MemoryStore -> T.DataKey -> MemoryStoreEntry -> IO ()
setMemoryDataKey (MemoryStore d w) k v = atomically $ modifyTVar' d (M.insert k v)

delMemoryDataKey :: MemoryStore -> T.DataKey -> IO Bool
delMemoryDataKey (MemoryStore d w) k = atomically $ do
  m <- readTVar d
  let existed = M.member k m
  writeTVar d (M.delete k m)
  pure existed

getMemoryWaitersVal :: MemoryStore -> T.ListKey -> IO (Maybe BLPopWaiters)
getMemoryWaitersVal (MemoryStore d w) k = M.lookup k <$> readTVarIO w

setMemoryWaitersKey :: MemoryStore -> T.ListKey -> BLPopWaiters -> IO ()
setMemoryWaitersKey (MemoryStore d w) k v = atomically $ modifyTVar' w (M.insert k v)

addMemoryWaiter :: MemoryStore -> T.ListKey -> T.ClientID -> IO ()
addMemoryWaiter store listKey cid = do
  val <- getMemoryWaitersVal store listKey
  case val of
    Nothing -> insertNewClient []
    Just xs -> unless (foldr checkEq False xs) $ insertNewClient xs
               where checkEq _ True = True
                     checkEq (BLPopWaiter curr) acc = curr == cid
  where insertNewClient ys = setMemoryWaitersKey store listKey (ys ++ [BLPopWaiter cid])

delMemoryWaitersKey :: MemoryStore -> T.ListKey -> IO Bool
delMemoryWaitersKey (MemoryStore d w) k = atomically $ do
  m <- readTVar w
  let existed = M.member k m
  writeTVar w (M.delete k m)
  pure existed
