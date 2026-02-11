module MemoryStore
  ( getSocket
  , getReplication
  , getClientReplication
  , getPort
  , getClientID
  , getMulti
  , getMultiList
  , updateMulti
  , addMultiCommand
  , resetMultiCommands
  , getData
  , getDataEntry
  , setDataEntry
  , addWaiterOnce
  , getWaiterEntry
  , delDataEntry
  , delWaiterEntry
  , newMemoryStore
  , getStreams
  , getStream
  , setStreams
  , getRole
  , MonadStore
  , getReplicas
  , addReplica
  , getReplicaSentOffset
  , setReplicaSentOffset
  , addMemberToZSet
  , getZSetMemberCount
  , getReplicaOffset
  , setReplicaOffset
  , addChannelSubcriber
  , removeChannelSubscriber
  , getChannelClients
  , getSubscribed
  , setSubscribed
  , removeSubChannel
  , getSubChannels
  , addSubChannel
  ) where

import Control.Concurrent.STM (atomically, TVar, readTVar, writeTVar, newTVarIO, readTVarIO, modifyTVar', modifyTVar)

import Control.Monad (when)
                     
import Data.Maybe (fromMaybe)

import Data.List (foldl')

import Types

import qualified Data.HashSet as HS
import qualified Data.Set as S
import qualified Data.HashMap.Strict as HM
import qualified Data.Map.Strict as M
import qualified Data.IntSet as IS

import Control.Monad.State.Strict
import Control.Monad.Reader

import Network.Simple.TCP (Socket)

import qualified Data.ByteString as BS

class (Monad m, MonadIO m) => MonadStore m where
  getData :: m (TVar (M.Map BS.ByteString MemoryStoreEntry))
  setDataEntry :: BS.ByteString -> MemoryStoreEntry -> m ()
  setDataEntry key value = do
    tv <- getData
    liftIO . atomically $ modifyTVar' tv (M.insert key value)
  getDataEntry :: BS.ByteString -> m (Maybe MemoryStoreEntry)
  getDataEntry key = do
    tv <- getData
    liftIO $ M.lookup key <$> readTVarIO tv
  getStream :: BS.ByteString -> (M.Map EntryId RedisStreamValues -> Maybe (EntryId, RedisStreamValues)) -> m (Maybe (EntryId, RedisStreamValues), RedisStream, RedisStreams)
  getStream streamID filter = do
    s@(Streams streams) <- getStreams
    let os@(Stream oldStream) = fromMaybe (Stream M.empty) (HM.lookup streamID streams)
    
    pure (filter oldStream, os, s)
  getStreams :: m RedisStreams
  getStreams = do
    streams <- getDataEntry "streams"
    pure $ case streams of
      Just (MemoryStoreEntry (MSStreams s) Nothing) -> s
      _ -> Streams HM.empty
  setStreams :: MemoryStoreEntry -> m ()
  setStreams = setDataEntry "streams"
  getPort :: m String
  getReplication :: m ReplicationInfo
  getReplicas :: m (TVar [Socket])
  addReplica :: Socket -> m ()
  addReplica sock = do
    tv <- getReplicas
    liftIO . atomically $ modifyTVar' tv (\l -> l ++ [sock])
  getReplicaSentOffset :: m Int
  setReplicaSentOffset :: Int -> m ()
  getZSet :: BS.ByteString -> m ZSet
  -- getZSet :: BS.ByteString -> m ZSetScoreMap
  getZSets :: m ZSets
  addMemberToZSet :: BS.ByteString -> Double -> BS.ByteString -> m ()
  addMemberToZSet setName score member = do
    tvSets <- getZSets
    (ZSet currSetMap currMemberDict) <- getZSet setName
    -- if a member is not a part of a score set yet, add it to the set, and update its memberDict to point to the score
    -- if a member is part of a score set, remove it from that set, add it to the (newly) relevant score set, and update its memberDict to reflect the new score
    case HM.lookup member currMemberDict of
      Just currScore -> when (currScore /= score) $ do
                           let newScoreMap = M.alter (Just . S.delete member . fromMaybe S.empty) currScore currSetMap
                           let updatedScoreMap = M.alter (Just . S.insert member . fromMaybe S.empty) score newScoreMap
                           let updatedMemberDict = HM.insert member score currMemberDict --HM.alter (Just . const score . fromMaybe 0) member currMemberDict
                           liftIO . atomically $
                               modifyTVar' tvSets (HM.alter (Just . const (ZSet updatedScoreMap updatedMemberDict) . fromMaybe (ZSet M.empty HM.empty)) setName)
      Nothing -> do
        let updatedScoreMap = M.alter (Just . S.insert member . fromMaybe S.empty) score currSetMap
        let updatedMemberDict = HM.alter (Just . const score . fromMaybe 0) member currMemberDict
        liftIO . atomically $
          modifyTVar' tvSets (HM.alter (Just . const (ZSet updatedScoreMap updatedMemberDict) . fromMaybe (ZSet M.empty HM.empty)) setName)
          
  getZSetMemberCount :: BS.ByteString -> BS.ByteString -> m Int
  getZSetMemberCount setName member = do
    (ZSet _ memberDict) <- getZSet setName
    case HM.lookup member memberDict of
      Just curr -> pure 1
      Nothing  -> pure 0

-------------------------------------------------------------------------------------

instance MonadStore ReplicaApp where
  getData = asks $ (.msData) . senvStore . renvShared
  getPort = asks $ (.cfgPort) . senvConfig . renvShared
  getReplication = asks $ (.cfgReplication) . senvConfig . renvShared
  getReplicas = asks $ senvReplicas . renvShared
  getReplicaSentOffset = do
    tv <- asks $ senvReplicaSentOffset . renvShared
    liftIO $ readTVarIO tv
  setReplicaSentOffset offset = do
    tv <- asks $ senvReplicaSentOffset . renvShared
    liftIO . atomically $ modifyTVar' tv (const offset)
  getZSet name = do
    tv <- asks $ senvSets . renvShared
    hmSets <- liftIO $ readTVarIO tv
    case HM.lookup name hmSets of
      Just s -> pure s
      -- Just (ZSet zsetMap zDict) -> pure zsetMap
      Nothing -> do
        let newSetScoreMap = M.empty -- create a new set, and return it
        let newSetMemberDict = HM.empty
        liftIO . atomically $ modifyTVar' tv (\curr -> HM.insert name (ZSet newSetScoreMap newSetMemberDict) hmSets)
        pure (ZSet newSetScoreMap newSetMemberDict)
  getZSets = asks $ senvSets . renvShared
  
instance MonadStore ClientApp where
  getData = asks $ (.msData) . senvStore . cenvShared
  getPort = asks $ (.cfgPort) . senvConfig . cenvShared
  getReplication = asks $ (.cfgReplication) . senvConfig . cenvShared
  getReplicas = asks (senvReplicas . cenvShared)
  getReplicaSentOffset = do
    tv <- asks $ senvReplicaSentOffset . cenvShared
    liftIO $ readTVarIO tv
  setReplicaSentOffset offset = do
    tv <- asks $ senvReplicaSentOffset . cenvShared
    liftIO . atomically $ modifyTVar' tv (const offset)
  getZSet name = do
    tv <- asks $ senvSets . cenvShared
    hmSets <- liftIO $ readTVarIO tv
    case HM.lookup name hmSets of
      Just s -> pure s
      -- Just (ZSet zsetMap zDict) -> pure zsetMap
      Nothing -> do
        let newSetScoreMap = M.empty -- create a new set, and return it
        let newSetMemberDict = HM.empty
        liftIO . atomically $ modifyTVar' tv (\curr -> HM.insert name (ZSet newSetScoreMap newSetMemberDict) hmSets)
        pure (ZSet newSetScoreMap newSetMemberDict)
  getZSets = asks $ senvSets . cenvShared

addChannelSubcriber :: BS.ByteString -> Socket -> ClientApp ()
addChannelSubcriber channel sub = do
  tv <- asks $ senvChannels . cenvShared
  liftIO . atomically $
    modifyTVar' tv (HM.alter (Just . addSub sub . fromMaybe []) channel)
  where
    addSub :: Socket -> [Socket] -> [Socket]
    addSub s curr
      | s `elem` curr = curr
      | otherwise     = s : curr

-- TODO: at some point, improve efficiency, don't use a list of sockets, use a set. a set does not have an Ord, so will need to store file descriptors, and socket <-> descriptor
removeChannelSubscriber :: BS.ByteString -> Socket -> ClientApp ()
removeChannelSubscriber channel sub = do
  tv <- asks $ senvChannels . cenvShared
  liftIO . atomically $ modifyTVar' tv (HM.alter (Just . removeSub sub . fromMaybe []) channel)
  where removeSub sub = foldl' (\b curr -> if curr == sub then b else curr : b) []

getChannelClients :: BS.ByteString -> ClientApp [Socket]
getChannelClients channel = do
  tv <- asks $ senvChannels . cenvShared
  hashmap <- liftIO $ readTVarIO tv
  case HM.lookup channel hashmap of
    Just l -> pure l
    Nothing -> pure []

getWaiters :: ClientApp (TVar (M.Map BS.ByteString IS.IntSet))
getWaiters = asks $ (.msBLPopWaiters) . senvStore . cenvShared

delDataEntry :: BS.ByteString -> ClientApp ()
delDataEntry key = do
  tv <- getData
  liftIO . atomically $ do
    m0 <- readTVar tv
    let m1 = M.delete key m0
    writeTVar tv m1

delWaiterEntry :: BS.ByteString -> ClientApp ()
delWaiterEntry key = do
  tv <- getWaiters
  liftIO . atomically $ do
    m0 <- readTVar tv
    let m1 = M.delete key m0
    writeTVar tv m1

addWaiterOnce :: BS.ByteString -> Int -> ClientApp ()
addWaiterOnce k w = do
  tv <- getWaiters
  liftIO . atomically $
    modifyTVar' tv (M.insertWith IS.union k (IS.singleton w))

getWaiterEntry :: BS.ByteString -> ClientApp (Maybe IS.IntSet)
getWaiterEntry key = do
  tv <- getWaiters
  liftIO $ M.lookup key <$> readTVarIO tv

getReplicaOffset :: ReplicaApp Int
getReplicaOffset = do
  tv <- asks $ (.renvReplicaOffset)
  liftIO $ readTVarIO tv

setReplicaOffset :: Int -> ReplicaApp ()
setReplicaOffset offset = do
  tv <- asks $ (.renvReplicaOffset)
  liftIO . atomically $ modifyTVar' tv (const offset)

getRole :: ClientApp ReplicationInfo
getRole = asks $ (.cfgReplication) . ccfgShared . cenvConfig

getClientID :: ClientApp Int
getClientID = asks $ (.ccfgID) . cenvConfig

getSocket :: ClientApp Socket
getSocket = asks $ (.ccfgSocket) . cenvConfig

getClientReplication :: ClientApp ReplicationInfo
getClientReplication = asks $ (.cfgReplication) . ccfgShared . cenvConfig


---------

newMemoryStore :: IO MemoryStore
newMemoryStore = MemoryStore <$> newTVarIO M.empty <*> newTVarIO M.empty

----------------------------------------------------------------------------------
-- ClientState

getSubscribed :: ClientApp Bool
getSubscribed = gets (.subscribeMode)

setSubscribed :: Bool -> ClientApp ()
setSubscribed sub = modify' (\cs -> cs { subscribeMode = sub })

removeSubChannel :: BS.ByteString -> ClientApp ()
removeSubChannel channel = do
  ml <- gets (.subscribeChannels)
  modify' (\cs -> cs { subscribeChannels = S.delete channel ml})

getSubChannels :: ClientApp (S.Set BS.ByteString)
getSubChannels = gets (.subscribeChannels)

addSubChannel :: BS.ByteString -> ClientApp ()
addSubChannel channel = do
  ml <- gets (.subscribeChannels)
  modify' (\cs -> cs { subscribeChannels = S.insert channel ml})

updateMulti :: Bool -> ClientApp ()
updateMulti state = modify' (\cs -> cs { multi = state })

addMultiCommand :: ClientApp Response -> ClientApp ()
addMultiCommand cmd = do
  ml <- gets (.multiList)
  modify' (\cs -> cs { multiList = ml ++ [cmd] })

resetMultiCommands :: ClientApp ()
resetMultiCommands = modify' (\cs -> cs { multiList = [] })

getMulti :: ClientApp Bool
getMulti = gets (.multi)

getMultiList :: ClientApp [ClientApp Response]
getMultiList = gets (.multiList)
