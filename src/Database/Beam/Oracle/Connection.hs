{-# LANGUAGE CPP                        #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE TypeFamilies               #-}

module Database.Beam.Oracle.Connection
  ( Oracle(..)
  , Ora(..)
  , runBeamOracle, runBeamOracleDebug
  ) where

import           Control.Exception
import           Control.Monad.Except
import           Control.Monad.State
import           Control.Monad.Reader
import           Control.Monad.Free.Church
import           Data.ByteString             (ByteString)
import qualified Data.ByteString.Char8       as B8
import qualified Data.ByteString.Lazy        as BL
import qualified Data.DList                  as DL
import           Data.Int
import           Data.Scientific             (Scientific)
import           Data.Text                   (Text)
import           Data.Time                   (LocalTime)
import           Data.Traversable            (for)
import           Data.Word
import           Database.Beam.Backend
import           Database.Beam.Query
import           Database.Beam.Query.SQL92   (buildSql92Query')
import           Database.Beam.Oracle.Syntax
import qualified Database.Odpi               as Odpi

data Oracle = Oracle

data OraCtx = OraCtx { logger     :: String -> IO ()
                     , connection :: Odpi.Connection }

newtype Ora a = Ora { runOra :: ReaderT OraCtx IO a }
  deriving (Functor, Applicative, Monad, MonadIO)

runBeamOracleDebug :: (String -> IO ()) -> Odpi.Connection -> Ora a -> IO a
runBeamOracleDebug logger connection act =
  runReaderT (runOra act) OraCtx{ logger, connection }

runBeamOracle :: Odpi.Connection -> Ora a -> IO a
runBeamOracle = runBeamOracleDebug (\_ -> pure ())

instance BeamBackend Oracle where
  type BackendFromField Oracle = Odpi.FromField

instance Odpi.FromField SqlNull where
  fromField _ (Odpi.NativeNull _) = Odpi.pureOk SqlNull
  fromField i v = Odpi.convError "SqlNull" i v

instance HasQBuilder Oracle where
  buildSqlQuery = buildSql92Query' True

instance BeamSqlBackend Oracle
type instance BeamSqlBackendSyntax Oracle = OraCommandSyntax

instance MonadBeam Oracle Ora where
  runReturningMany :: FromBackendRow Oracle x
                   => OraCommandSyntax
                   -> (Ora (Maybe x) -> Ora a)
                   -> Ora a
  runReturningMany OraCommandSyntax{ fromOraCommand = OraSyntax cmd vals }
                   consume = Ora $ do
    ctx@OraCtx{ logger, connection } <- ask
    let cmdStr = BL.toStrict $ withPlaceholders cmd
    liftIO $ do
      logger (B8.unpack cmdStr ++ ";\n-- With values: " ++ show (DL.toList vals))
      Odpi.withStatement connection False cmdStr $ \st -> do
        bindValues st vals
        ncol <- Odpi.stmtExecute st Odpi.ModeExecDefault
        -- Odpi.defineValuesForRow (Proxy :: Proxy x) st
        runReaderT (runOra (consume $ nextRow st ncol)) ctx
    where
      bindValues :: Odpi.Statement -> DL.DList Odpi.NativeValue -> IO ()
      bindValues st vs = mapM_ (bindOne st) $ zip [1..] (DL.toList vs)

      bindOne st (pos, v) = Odpi.stmtBindValueByPos st pos v

-- action that fetches the next row passed to consume
nextRow :: FromBackendRow Oracle x => Odpi.Statement -> Word32 -> Ora (Maybe x)
nextRow st ncol = Ora $ liftIO $ do
  mPageOffset <- Odpi.stmtFetch st
  case mPageOffset of
    Nothing -> pure Nothing
    Just _ -> do
      fields <- for [1..ncol] $ \n -> do
        qi <- Odpi.stmtGetQueryInfo st n
        nv <- Odpi.stmtGetQueryValue st n
        pure (qi, nv)
      let initialState = RowParserState{ fields, curCol = 0 }
      (res, _) <- runRowParser initialState $ runFromBackendRowM fromBackendRow pure step
      case res of
        Right x -> pure x
        Left err -> throwIO err

runFromBackendRowM :: FromBackendRowM be a
                   -> (a -> r)
                   -> (FromBackendRowF be r -> r)
                   -> r
runFromBackendRowM (FromBackendRowM act) finish step = runF act finish step

data RowParserState = RowParserState { fields :: [(Odpi.QueryInfo, Odpi.NativeValue)]
                                     , curCol :: Int }

type RowParser a = ExceptT BeamRowReadError (StateT RowParserState IO) a

runRowParser :: RowParserState
             -> RowParser a
             -> IO (Either BeamRowReadError a, RowParserState)
runRowParser state parser = runStateT (runExceptT parser) state

nextField :: RowParser (Odpi.QueryInfo, Odpi.NativeValue)
nextField = do
  RowParserState{ fields, curCol } <- get
  case fields of
    f : fs -> do
      put RowParserState{ fields = fs, curCol = curCol + 1 }
      pure f
    [] -> throwError err
      where
        err = BeamRowReadError
          { brreColumn = Just curCol
          , brreError = ColumnNotEnoughColumns curCol
          }

getCurCol :: RowParser Int
getCurCol = curCol <$> get

step :: FromBackendRowF Oracle (RowParser a) -> RowParser a
step = \case
  ParseOneField next -> do
    curCol <- getCurCol
    (qi, nv) <- nextField
    res <- liftIO $ Odpi.fromField qi nv
    case res of
      Odpi.Ok x -> next x
      Odpi.Errors errs -> throwError $ translateErrors curCol errs
  Alt (FromBackendRowM a) (FromBackendRowM b) next -> do
    let a' = runF a pure step
    let b' = runF b pure step
    state <- get
    (resA, stateA) <- liftIO $ runRowParser state a'
    case resA of
      Right x -> do
        put stateA
        next x
      Left errA -> do
        (resB, stateB) <- liftIO $ runRowParser state b'
        case resB of
          Right y -> do
            put stateB
            next y
          Left errB ->
            throwError $ mergeErrors errA errB
  FailParseWith err ->
    throwError err
  where
    -- TODO: Do something better
    translateErrors curCol errs =
      BeamRowReadError
      { brreColumn = Just curCol
      , brreError = ColumnErrorInternal $ unlines $ map displayException errs
      }

    -- TODO: Do something better
    mergeErrors _ errB = errB

#define HAS_ORACLE_EQUALITY_CHECK(ty) \
  instance HasSqlEqualityCheck Oracle (ty); \
  instance HasSqlQuantifiedEqualityCheck Oracle (ty);

HAS_ORACLE_EQUALITY_CHECK(Int32)
HAS_ORACLE_EQUALITY_CHECK(Text)

instance FromBackendRow Oracle SqlNull
instance FromBackendRow Oracle Bool
instance FromBackendRow Oracle Word
instance FromBackendRow Oracle Word16
instance FromBackendRow Oracle Word32
instance FromBackendRow Oracle Word64
instance FromBackendRow Oracle Int
instance FromBackendRow Oracle Int16
instance FromBackendRow Oracle Int32
instance FromBackendRow Oracle Int64
instance FromBackendRow Oracle Float
instance FromBackendRow Oracle Double
instance FromBackendRow Oracle Scientific
instance FromBackendRow Oracle Char
instance FromBackendRow Oracle ByteString
instance FromBackendRow Oracle Text
instance FromBackendRow Oracle LocalTime
