module Store
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
  , getStream
  , getData
  , getDataEntry
  , delDataEntry
  , setDataEntry
  , newMemoryStore
  , getStreams
  , getTVStreams
  , setStreams
  , getRole
  , MonadStore
  , getReplicas
  , addReplica
  , getReplicaSentOffset
  , setReplicaSentOffset
  , addMemberToZSet
  , getZSet
  , getZSets
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

import Control.Monad.Trans.Except (ExceptT (..))

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



type StreamFilter = Stream -> Maybe (EntryId, [BSPair])

getStream :: Streams -> BS.ByteString -> StreamFilter -> (Maybe (EntryId, [BSPair]), Stream, Streams)
getStream streams streamID filter = let stream = fromMaybe M.empty $ HM.lookup streamID streams
                                    in (filter stream, stream, streams)


class (Monad m, MonadIO m) => MonadStore m where
  getData :: m (TVar StoreData)
  getTVStreams :: m (TVar Streams)
  getStreams :: m Streams
  getStreams = do
    tvStreams <- getTVStreams
    liftIO $ readTVarIO tvStreams
  setDataEntry :: BS.ByteString -> StoreEntry -> m ()
  setDataEntry key value = do
    tv <- getData
    liftIO . atomically $ modifyTVar' tv (M.insert key value)
  getDataEntry :: BS.ByteString -> m (Maybe StoreEntry)
  getDataEntry key = do
    tv <- getData
    liftIO $ M.lookup key <$> readTVarIO tv
  setStreams :: Streams -> m ()
  setStreams newStreams = do
    tvStreams <- getTVStreams
    liftIO . atomically $
      modifyTVar' tvStreams (const newStreams)
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
  getData = asks $ (.sData) . senvStore . renvShared
  getTVStreams = asks $ (.sStreams) . senvStore . renvShared
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
    liftIO . atomically $ do
      hmSets <- readTVar tv
      case HM.lookup name hmSets of
        Just s -> pure s
        -- Just (ZSet zsetMap zDict) -> pure zsetMap
        Nothing -> do
          let newSetScoreMap = M.empty -- create a new set, and return it
          let newSetMemberDict = HM.empty
          modifyTVar' tv (HM.insert name (ZSet newSetScoreMap newSetMemberDict))
          pure (ZSet newSetScoreMap newSetMemberDict)
  getZSets = asks $ senvSets . renvShared
  
instance MonadStore ClientApp where
  getData = asks $ (.sData) . senvStore . cenvShared
  getTVStreams = asks $ (.sStreams) . senvStore . cenvShared
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
    liftIO . atomically $ do
      hmSets <- readTVar tv
      case HM.lookup name hmSets of
        Just s -> pure s
        -- Just (ZSet zsetMap zDict) -> pure zsetMap
        Nothing -> do
          let newSetScoreMap = M.empty -- create a new set, and return it
          let newSetMemberDict = HM.empty
          modifyTVar' tv (HM.insert name (ZSet newSetScoreMap newSetMemberDict))
          pure (ZSet newSetScoreMap newSetMemberDict)
  getZSets = asks $ senvSets . cenvShared

instance MonadStore m => MonadStore (ExceptT e m) where
  getData = lift getData
  getTVStreams = lift getTVStreams
  getPort = lift getPort
  getReplication = lift getReplication
  getReplicas = lift getReplicas
  getReplicaSentOffset = lift getReplicaSentOffset
  setReplicaSentOffset = lift . setReplicaSentOffset
  getZSet = lift . getZSet
  getZSets = lift getZSets

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

delDataEntry :: BS.ByteString -> ClientApp ()
delDataEntry key = do
  tv <- getData
  liftIO . atomically $ do
    m0 <- readTVar tv
    let m1 = M.delete key m0
    writeTVar tv m1

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

newMemoryStore :: IO Store
newMemoryStore = Store <$> newTVarIO M.empty <*> newTVarIO HM.empty

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
