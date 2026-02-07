{-# LANGUAGE OverloadedStrings #-}

module CliParser
  ( parseConfig
  ) where

import Types

import Options.Applicative

parseConfig :: IO Config
parseConfig = execParser $ info (configP <**> helper) $
  fullDesc <> progDesc "Redis clone"

configP :: Parser Config
configP =
  Config
    <$> optional (option str (long "port" <> metavar "PORT" <> help "Port to listen on"))
    <*> optional replicaOfP

replicaOfP :: Parser ReplicationInfo
replicaOfP =
  option (eitherReader parse)
    (long "replicaof" <> metavar "HOST PORT" <> help "Replica of upstream HOST PORT")
 where
  parse s = case words s of
    [h,p] -> Right (Slave h p)
    _     -> Left "Expected: --replicaof \"HOST PORT\""
