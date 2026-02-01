module Types
  ( ExpireDuration (..)
  , ExpireReference (..)
  , ClientID
  , DataKey
  , CommandArguments
  , ListKey
  , BLPopResponse
  ) where

import qualified Data.ByteString as BS

newtype ExpireDuration = ExpireDuration Integer deriving (Eq, Show)  -- in miliseconds
newtype ExpireReference = ExpireReference Integer deriving (Eq, Show)

type ClientID = Int
type DataKey = BS.ByteString
type CommandArguments = [BS.ByteString]
type ListKey = BS.ByteString
type BLPopResponse = BS.ByteString
