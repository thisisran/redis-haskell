module Types
  ( ExpireDuration (..)
  , ExpireReference (..)
  ) where

newtype ExpireDuration = ExpireDuration Integer deriving (Eq, Show)  -- in miliseconds
newtype ExpireReference = ExpireReference Integer deriving (Eq, Show)
