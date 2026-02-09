{-# LANGUAGE OverloadedStrings #-}

module CliParser
  ( parseCli
  ) where

import Types

import Options.Applicative

parseCli :: IO CLIOptions
parseCli = execParser $ info (configP <**> helper) $
  fullDesc <> progDesc "Redis clone"

configP :: Parser CLIOptions
configP =
  CLIOptions
    <$> optional (option str (long "port" <> metavar "PORT" <> help "Port to listen on"))
    <*> optional (option str (long "dir" <> metavar "DIR" <> help "RDB file directory"))
    <*> optional (option str (long "dbfilename" <> metavar "DBFILENAME" <> help "RDB file name"))
    <*> replicaOfP

replicaOfP :: Parser ReplicationCmdOption
replicaOfP =
  option (eitherReader parse)
    (long "replicaof" <> metavar "HOST PORT" <> help "Replica of upstream HOST PORT")
    <|> pure WantMaster
 where
  parse s = case words s of
    [h,p] -> Right (WantSlave h p)
    _     -> Left "Expected: --replicaof \"HOST PORT\""
