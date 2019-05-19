{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE TypeFamilies          #-}

module Database.Beam.Oracle.Syntax where

import           Control.Monad.State

import qualified Data.ByteString.Lazy               as BL
import           Data.ByteString.Builder            (Builder)
import qualified Data.ByteString.Builder            as BB
import           Data.ByteString.Builder.Scientific (scientificBuilder)
import           Data.DList                         (DList)
import qualified Data.DList                         as DL
import           Data.Fixed
import           Data.Int
import           Data.List                          (intersperse)
import           Data.Scientific                    (Scientific)
import           Data.String                        (IsString(..))
import           Data.Text                          (Text)
import qualified Data.Text                          as T
import qualified Data.Text.Encoding                 as TE
import qualified Data.Text.Lazy                     as TL
import           Data.Time                          (Day, TimeOfDay)
import qualified Data.Time                          as Ti
import           Data.Word

import           Database.Beam.Backend.SQL

import qualified Database.Odpi                      as Odpi

data OraSyntax = OraSyntax (forall m. Monad m => (Odpi.NativeValue -> m Builder) -> m Builder)
                           (DList Odpi.NativeValue)

instance Eq OraSyntax where
  OraSyntax ab av == OraSyntax bb bv =
    withPlaceholders ab == withPlaceholders bb
    && av == bv

instance Show OraSyntax where
  show (OraSyntax build vals) =
    "OraSyntax " <> show (withPlaceholders build) <> " " <> show vals

instance Semigroup OraSyntax where
  OraSyntax ab av <> OraSyntax bb bv =
    OraSyntax (\v -> (<>) <$> ab v <*> bb v) (av <> bv)

instance Monoid OraSyntax where
  mempty = OraSyntax (\_ -> pure mempty) mempty
  mappend = (<>)

instance IsString OraSyntax where
  fromString = emit . fromString
    
-- | Convert the first argument of 'OraSyntax' to a lazy 'ByteString',
-- where all the data has been replaced by @":x"@ placeholders.
withPlaceholders :: ((Odpi.NativeValue -> State Int Builder) -> State Int Builder)
                 -> BL.ByteString
withPlaceholders build = BB.toLazyByteString $ evalState (build $ \_ -> nextId) 1
  where
    nextId = do
      i <- get
      put $ i + 1
      pure $ ":" <> BB.intDec i

data OraCommandType
  = OraCommandTypeQuery
  | OraCommandTypeDdl
  | OraCommandTypeDataUpdate
  | OraCommandTypeDataUpdateReturning

data OraCommandSyntax
  = OraCommandSyntax
  { oraCommandType :: OraCommandType
  , fromOraCommand :: OraSyntax
  }

newtype OraSelectSyntax = OraSelectSyntax { fromOraSelect :: OraSyntax }
newtype OraInsertSyntax = OraInsertSyntax { fromOraInsert :: OraSyntax }
newtype OraUpdateSyntax = OraUpdateSyntax { fromOraUpdate :: OraSyntax }
newtype OraDeleteSyntax = OraDeleteSyntax { fromOraDelete :: OraSyntax }
newtype OraSelectTableSyntax = OraSelectTableSyntax { fromOraSelectTable :: OraSyntax }
newtype OraOrderingSyntax = OraOrderingSyntax { fromOraOrdering :: OraSyntax }
newtype OraTableNameSyntax = OraTableNameSyntax { fromOraTableName :: OraSyntax }
newtype OraInsertValuesSyntax = OraInsertValuesSyntax { fromOraInsertValues :: OraSyntax }
newtype OraFieldNameSyntax = OraFieldNameSyntax { fromOraFieldName :: OraSyntax }
newtype OraExpressionSyntax = OraExpressionSyntax { fromOraExpression :: OraSyntax } deriving Eq
newtype OraProjectionSyntax = OraProjectionSyntax { fromOraProjection :: OraSyntax }
newtype OraFromSyntax = OraFromSyntax { fromOraFromSyntax :: OraSyntax }
newtype OraGroupingSyntax = OraGroupingSyntax { fromOraGrouping :: OraSyntax }
newtype OraAggregationSetQuantifierSyntax = OraAggregationSetQuantifierSyntax { fromOraAggregationSetQuantifier :: OraSyntax }
newtype OraValueSyntax = OraValueSyntax { fromOraValue :: OraSyntax }
newtype OraComparisonQuantifierSyntax = OraComparisonQuantifierSyntax { fromOraComparisonQuantifier :: OraSyntax }

data OraDataTypeDescr
  = OraDataTypeDescrDomain Text
  | OraDataTypeDescr
  { oraTyName :: Text
  , oraTyLength :: Maybe Word
  , oraTyPrecision :: Maybe Word
  , oraTyScale :: Maybe Int
  }

data OraDataTypeSyntax
  = OraDataTypeSyntax
  { oraDataTypeDescr :: OraDataTypeDescr
  , fromOraDataType :: OraSyntax
  -- , oraDataTypeSerialized :: BeamSerializedDataType
  }
newtype OraExtractFieldSyntax = OraExtractFieldSyntax { fromOraExtractField :: OraSyntax }
newtype OraTableSourceSyntax = OraTableSourceSyntax { fromOraTableSource :: OraSyntax }

-- newtype OraSetQuantifierSyntax = OraSetQuantifierSyntax { fromOraSetQuantifier :: OraSyntax }
-- newtype OraTableSourceSyntax = OraTableSourceSyntax { fromOraTableSource :: OraSyntax }
-- newtype OraExtractFieldSyntax = OraExtractFieldSyntax { fromOraExtractField :: OraSyntax }

instance IsSql92Syntax OraCommandSyntax where
  type Sql92SelectSyntax OraCommandSyntax = OraSelectSyntax
  type Sql92InsertSyntax OraCommandSyntax = OraInsertSyntax
  type Sql92UpdateSyntax OraCommandSyntax = OraUpdateSyntax
  type Sql92DeleteSyntax OraCommandSyntax = OraDeleteSyntax

  selectCmd = OraCommandSyntax OraCommandTypeQuery . fromOraSelect
  insertCmd = OraCommandSyntax OraCommandTypeDataUpdate . fromOraInsert
  updateCmd = OraCommandSyntax OraCommandTypeDataUpdate . fromOraUpdate
  deleteCmd = OraCommandSyntax OraCommandTypeDataUpdate . fromOraDelete

instance IsSql92SelectSyntax OraSelectSyntax where
  type Sql92SelectSelectTableSyntax OraSelectSyntax = OraSelectTableSyntax
  type Sql92SelectOrderingSyntax OraSelectSyntax = OraOrderingSyntax

  selectStmt :: OraSelectTableSyntax
             -> [OraOrderingSyntax]
             -> Maybe Integer
             -> Maybe Integer
             -> OraSelectSyntax
  selectStmt tbl ordering limit offset =
    OraSelectSyntax $
    case (limit, offset) of
      (Just limit', Just offset') ->
          " SELECT * FROM (SELECT a.*, ROWNUM as rnum FROM " <> oraParens sql <>
          " a WHERE ROWNUM <= " <> emit (BB.integerDec $ offset' + limit') <>
          ") WHERE rnum > " <> emit (BB.integerDec offset')
      (Just limit', Nothing) ->
          " SELECT * FROM " <> oraParens sql <> " WHERE ROWNUM <= " <>
          emit (BB.integerDec limit')
      (Nothing, Just offset') ->
          " SELECT * FROM (SELECT a.*, ROWNUM as rnum FROM " <> oraParens sql <>
          " a) WHERE rnum > " <> emit (BB.integerDec offset')
      _ -> sql
    where
      sql = fromOraSelectTable tbl <>
            (case ordering of
              [] -> mempty
              _  -> " ORDER BY " <> (oraCommas $ map fromOraOrdering ordering))

instance IsSql92InsertSyntax OraInsertSyntax where
  type Sql92InsertTableNameSyntax OraInsertSyntax = OraTableNameSyntax
  type Sql92InsertValuesSyntax OraInsertSyntax = OraInsertValuesSyntax

  insertStmt tblName fields values =
    OraInsertSyntax $
    "INSERT INTO " <> fromOraTableName tblName <>
    (oraParens $ oraCommas $ map oraIdentifier fields) <>
    fromOraInsertValues values

instance IsSql92UpdateSyntax OraUpdateSyntax where
  type Sql92UpdateTableNameSyntax OraUpdateSyntax = OraTableNameSyntax
  type Sql92UpdateFieldNameSyntax OraUpdateSyntax = OraFieldNameSyntax
  type Sql92UpdateExpressionSyntax OraUpdateSyntax = OraExpressionSyntax

  updateStmt tblName fields where_ =
    OraUpdateSyntax $
    "UPDATE " <> fromOraTableName tblName <>
    (case fields of
       [] -> mempty
       _ ->
         " SET " <>
         oraCommas (map (\(field, val) -> fromOraFieldName field <> "=" <>
                                          fromOraExpression val) fields)) <>
    memptyOr (\where' -> " WHERE " <> fromOraExpression where') where_

instance IsSql92DeleteSyntax OraDeleteSyntax where
  type Sql92DeleteTableNameSyntax OraDeleteSyntax = OraTableNameSyntax
  type Sql92DeleteExpressionSyntax OraDeleteSyntax = OraExpressionSyntax

  deleteStmt tblName alias where_ =
    OraDeleteSyntax $
    "DELETE FROM " <> fromOraTableName tblName <>
    memptyOr (\alias_ -> " AS " <> oraIdentifier alias_) alias <>
    memptyOr (\where' -> " WHERE " <> fromOraExpression where') where_

  deleteSupportsAlias _ = True

instance IsSql92SelectTableSyntax OraSelectTableSyntax where
  type Sql92SelectTableSelectSyntax OraSelectTableSyntax = OraSelectSyntax
  type Sql92SelectTableExpressionSyntax OraSelectTableSyntax = OraExpressionSyntax
  type Sql92SelectTableProjectionSyntax OraSelectTableSyntax = OraProjectionSyntax
  type Sql92SelectTableFromSyntax OraSelectTableSyntax = OraFromSyntax
  type Sql92SelectTableGroupingSyntax OraSelectTableSyntax = OraGroupingSyntax
  type Sql92SelectTableSetQuantifierSyntax OraSelectTableSyntax = OraAggregationSetQuantifierSyntax

  selectTableStmt :: Maybe OraAggregationSetQuantifierSyntax
                  -> OraProjectionSyntax
                  -> Maybe OraFromSyntax
                  -> Maybe OraExpressionSyntax
                  -> Maybe OraGroupingSyntax
                  -> Maybe OraExpressionSyntax
                  -> OraSelectTableSyntax
  selectTableStmt setQuantifier proj from where_ grouping having =
    OraSelectTableSyntax $
    "SELECT " <>
    memptyOr (\sq -> fromOraAggregationSetQuantifier sq <> " ") setQuantifier <>
    fromOraProjection proj <>
    memptyOr (" FROM " <>) (fmap fromOraFromSyntax from) <>
    memptyOr (" WHERE " <>) (fmap fromOraExpression where_) <>
    memptyOr (" GROUP BY " <>) (fmap fromOraGrouping grouping) <>
    memptyOr (" HAVING " <>) (fmap fromOraExpression having)

  unionTables True  = oraTblOp "UNION ALL"
  unionTables False = oraTblOp "UNION"
  
  intersectTables _ = oraTblOp "INTERSECT"

  exceptTable _ = oraTblOp "EXCEPT"

oraTblOp :: OraSyntax
         -> OraSelectTableSyntax
         -> OraSelectTableSyntax
         -> OraSelectTableSyntax
oraTblOp op a b =
  OraSelectTableSyntax $ fromOraSelectTable a <> " " <> op <> " " <> fromOraSelectTable b

instance IsSql92OrderingSyntax OraOrderingSyntax where
  type Sql92OrderingExpressionSyntax OraOrderingSyntax = OraExpressionSyntax

  ascOrdering :: OraExpressionSyntax -> OraOrderingSyntax
  ascOrdering e = OraOrderingSyntax $ fromOraExpression e <> " ASC"

  descOrdering :: OraExpressionSyntax -> OraOrderingSyntax
  descOrdering e = OraOrderingSyntax $ fromOraExpression e <> " DESC"

instance IsSql92InsertValuesSyntax OraInsertValuesSyntax where
  type Sql92InsertValuesExpressionSyntax OraInsertValuesSyntax = OraExpressionSyntax
  type Sql92InsertValuesSelectSyntax OraInsertValuesSyntax = OraSelectSyntax

  insertSqlExpressions es =
    OraInsertValuesSyntax $
    "VALUES " <>
    (oraCommas $ map (oraParens . oraCommas . map fromOraExpression) es)

  insertFromSql a = OraInsertValuesSyntax (fromOraSelect a)

instance IsSql92TableNameSyntax OraTableNameSyntax where
  tableName :: Maybe Text -> Text -> OraTableNameSyntax
  tableName maybeSchema tbl =
    OraTableNameSyntax $
    memptyOr (\schema -> oraIdentifier schema <> ".") maybeSchema <>
    oraIdentifier tbl

instance IsSql92ExpressionSyntax OraExpressionSyntax where
  type Sql92ExpressionValueSyntax OraExpressionSyntax = OraValueSyntax
  type Sql92ExpressionSelectSyntax OraExpressionSyntax = OraSelectSyntax
  type Sql92ExpressionFieldNameSyntax OraExpressionSyntax = OraFieldNameSyntax
  type Sql92ExpressionQuantifierSyntax OraExpressionSyntax = OraComparisonQuantifierSyntax
  type Sql92ExpressionCastTargetSyntax OraExpressionSyntax = OraDataTypeSyntax
  type Sql92ExpressionExtractFieldSyntax OraExpressionSyntax = OraExtractFieldSyntax

  addE = oraBinOp "+"
  subE = oraBinOp "-"
  mulE = oraBinOp "*"
  divE = oraBinOp "/"
  modE = oraBinOp "%"

  orE = oraBinOp "OR"
  andE = oraBinOp "AND"
  likeE = oraBinOp "LIKE"
  overlapsE = oraBinOp "OVERLAPS"

  eqE :: Maybe OraComparisonQuantifierSyntax
      -> OraExpressionSyntax
      -> OraExpressionSyntax
      -> OraExpressionSyntax
  eqE = oraCompOp "="
  neqE = oraCompOp "<>"
  ltE = oraCompOp "<"
  gtE = oraCompOp ">"
  leE = oraCompOp "<="
  geE = oraCompOp ">="

  negateE = oraUnOp "-"
  notE = oraUnOp "NOT"

  eqMaybeE a b e = (isNullE a `andE` isNullE b) `orE` e
  neqMaybeE a b e = notE $ eqMaybeE a b (notE e)

  existsE s = OraExpressionSyntax $ "EXISTS(" <> fromOraSelect s <> ")"
  uniqueE s = OraExpressionSyntax $ "UNIQUE(" <> fromOraSelect s <> ")"

  isNotNullE = oraPostFix "IS NOT NULL"
  isNullE = oraPostFix "IS NULL"
  isTrueE = oraPostFix "IS TRUE"
  isFalseE = oraPostFix "IS FALSE"
  isNotTrueE = oraPostFix "IS NOT TRUE"
  isNotFalseE = oraPostFix "IS NOT FALSE"
  isUnknownE = oraPostFix "IS UNKNOWN"
  isNotUnknownE = oraPostFix "IS NOT UNKNOWN"

  betweenE a b c =
    OraExpressionSyntax $
    oraParens (fromOraExpression a) <> " BETWEEN " <>
    oraParens (fromOraExpression b) <> " AND " <>
    oraParens (fromOraExpression c)

  valueE :: OraValueSyntax -> OraExpressionSyntax
  valueE = OraExpressionSyntax . fromOraValue
  rowE = OraExpressionSyntax . oraParens . oraCommas . map fromOraExpression
  fieldE :: OraFieldNameSyntax -> OraExpressionSyntax
  fieldE = OraExpressionSyntax . fromOraFieldName
  subqueryE = OraExpressionSyntax . oraParens . fromOraSelect

  positionE needle haystack =
    OraExpressionSyntax $
    "POSITION((" <> fromOraExpression needle <> ") IN (" <>
    fromOraExpression haystack <> "))"
  nullIfE a b =
    OraExpressionSyntax $
    "NULLIF(" <> fromOraExpression a <> ", " <>
    fromOraExpression b <> ")"
  absE a = OraExpressionSyntax $ "ABS" <> oraParens (fromOraExpression a)
  bitLengthE a = OraExpressionSyntax $ "BIT_LENGTH" <> oraParens (fromOraExpression a)
  charLengthE a = OraExpressionSyntax $ "CHAR_LENGTH" <> oraParens (fromOraExpression a)
  octetLengthE a = OraExpressionSyntax $ "OCTET_LENGTH" <> oraParens (fromOraExpression a)
  coalesceE :: [OraExpressionSyntax] -> OraExpressionSyntax
  coalesceE es =
    OraExpressionSyntax $
    "COALESCE" <> (oraParens $ oraCommas $ map fromOraExpression es)
  extractE field from =
    OraExpressionSyntax $
    "EXTRACT" <> oraParens (fromOraExtractField field) <>
    " FROM " <> oraParens (fromOraExpression from)
  castE e to =
    OraExpressionSyntax $
    "CAST " <> oraParens (fromOraExpression e) <> " AS " <>
    oraParens (fromOraDataType to)
  caseE cases else' =
    OraExpressionSyntax $
    "CASE " <>
    foldMap (\(cond, res) -> "WHEN " <> fromOraExpression cond <>
                             " THEN " <> fromOraExpression res <>
                             " ") cases <>
    "ELSE " <> fromOraExpression else' <> " END"

  currentTimestampE = OraExpressionSyntax "CURRENT_TIMESTAMP"
  defaultE = OraExpressionSyntax "DEFAULT"

  inE e es =
    OraExpressionSyntax $
    oraParens (fromOraExpression e) <> " IN " <>
    (oraParens $ oraCommas $ map fromOraExpression es)

  trimE x = OraExpressionSyntax $ "TRIM" <> oraParens (fromOraExpression x)
  lowerE x = OraExpressionSyntax $ "LOWER" <> oraParens (fromOraExpression x)
  upperE x = OraExpressionSyntax $ "UPPER" <> oraParens (fromOraExpression x)

oraUnOp :: OraSyntax -> OraExpressionSyntax -> OraExpressionSyntax
oraUnOp op e = OraExpressionSyntax $ op <> " " <> oraParens (fromOraExpression e)

oraPostFix :: OraSyntax -> OraExpressionSyntax -> OraExpressionSyntax
oraPostFix op e =
  OraExpressionSyntax $ oraParens (fromOraExpression e) <> " " <> op

oraCompOp :: OraSyntax
          -> Maybe OraComparisonQuantifierSyntax
          -> OraExpressionSyntax -> OraExpressionSyntax
          -> OraExpressionSyntax
oraCompOp op quantifier a b =
    OraExpressionSyntax $
    oraParens (fromOraExpression a) <> " " <> op <> " " <>
    memptyOr (\q -> " " <> fromOraComparisonQuantifier q <> " ") quantifier <>
    oraParens (fromOraExpression b)

oraBinOp :: OraSyntax
         -> OraExpressionSyntax
         -> OraExpressionSyntax
         -> OraExpressionSyntax
oraBinOp op a b =
  OraExpressionSyntax $
  oraParens (fromOraExpression a) <> " " <> op <> " " <> oraParens (fromOraExpression b)

instance IsSql92AggregationExpressionSyntax OraExpressionSyntax where
  type Sql92AggregationSetQuantifierSyntax OraExpressionSyntax = OraAggregationSetQuantifierSyntax

  countAllE :: OraExpressionSyntax
  countAllE = OraExpressionSyntax "COUNT(*)"

  countE :: Maybe OraAggregationSetQuantifierSyntax
         -> OraExpressionSyntax
         -> OraExpressionSyntax
  countE = oraUnAgg "COUNT"

  avgE :: Maybe OraAggregationSetQuantifierSyntax
       -> OraExpressionSyntax
       -> OraExpressionSyntax
  avgE = oraUnAgg "AVG"

  sumE :: Maybe OraAggregationSetQuantifierSyntax
       -> OraExpressionSyntax
       -> OraExpressionSyntax
  sumE = oraUnAgg "SUM"

  minE :: Maybe OraAggregationSetQuantifierSyntax
       -> OraExpressionSyntax
       -> OraExpressionSyntax
  minE = oraUnAgg "MIN"

  maxE :: Maybe OraAggregationSetQuantifierSyntax
       -> OraExpressionSyntax
       -> OraExpressionSyntax
  maxE = oraUnAgg "MAX"

oraUnAgg :: OraSyntax
         -> Maybe OraAggregationSetQuantifierSyntax
         -> OraExpressionSyntax
         -> OraExpressionSyntax
oraUnAgg fn q e =
    OraExpressionSyntax $
    fn <>
    oraParens
      (memptyOr (\q' -> fromOraAggregationSetQuantifier q' <> " ") q <>
       fromOraExpression e)


instance IsSql92ProjectionSyntax OraProjectionSyntax where
  type Sql92ProjectionExpressionSyntax OraProjectionSyntax = OraExpressionSyntax

  projExprs :: [(OraExpressionSyntax, Maybe Text)] -> OraProjectionSyntax
  projExprs exprs =
    OraProjectionSyntax $
    oraCommas $ 
    map (\(expr, nm) ->
            fromOraExpression expr <>
            memptyOr (\nm' -> " AS " <> oraIdentifier nm') nm)
        exprs


instance IsSql92FromSyntax OraFromSyntax where
  type Sql92FromExpressionSyntax OraFromSyntax = OraExpressionSyntax
  type Sql92FromTableSourceSyntax OraFromSyntax = OraTableSourceSyntax

  fromTable :: OraTableSourceSyntax -> Maybe (Text, Maybe [Text]) -> OraFromSyntax
  fromTable tableSrc = \case
    Nothing -> OraFromSyntax (fromOraTableSource tableSrc)
    Just (nm, colNms) -> 
      OraFromSyntax $
      fromOraTableSource tableSrc <> " " <> oraIdentifier nm <>
      memptyOr (oraParens . oraCommas . map oraIdentifier) colNms

  innerJoin :: OraFromSyntax -> OraFromSyntax -> Maybe OraExpressionSyntax -> OraFromSyntax
  innerJoin = oraJoin "JOIN"
  
  leftJoin :: OraFromSyntax -> OraFromSyntax -> Maybe OraExpressionSyntax -> OraFromSyntax
  leftJoin = oraJoin "LEFT JOIN"

  rightJoin :: OraFromSyntax -> OraFromSyntax -> Maybe OraExpressionSyntax -> OraFromSyntax
  rightJoin = oraJoin "RIGHT JOIN"

oraJoin :: OraSyntax
        -> OraFromSyntax
        -> OraFromSyntax
        -> Maybe OraExpressionSyntax
        -> OraFromSyntax
oraJoin joinType a b = \case
  Just e ->
    OraFromSyntax (fromOraFromSyntax a <> " " <> joinType <> " " <>
                   fromOraFromSyntax b <> " ON " <> fromOraExpression e)
  Nothing ->
    OraFromSyntax (fromOraFromSyntax a <> " " <> joinType <> " " <> fromOraFromSyntax b)

instance IsSql92GroupingSyntax OraGroupingSyntax where
  type Sql92GroupingExpressionSyntax OraGroupingSyntax = OraExpressionSyntax

  groupByExpressions = OraGroupingSyntax . oraCommas . map fromOraExpression

instance IsSql92AggregationSetQuantifierSyntax OraAggregationSetQuantifierSyntax where
  setQuantifierDistinct = OraAggregationSetQuantifierSyntax "DISTINCT"
  setQuantifierAll = OraAggregationSetQuantifierSyntax "ALL"

instance IsSql92QuantifierSyntax OraComparisonQuantifierSyntax where
  quantifyOverAll = OraComparisonQuantifierSyntax "ALL"
  quantifyOverAny = OraComparisonQuantifierSyntax "ANY"

instance IsSql92DataTypeSyntax OraDataTypeSyntax where
  domainType nm = OraDataTypeSyntax
    { oraDataTypeDescr = OraDataTypeDescrDomain nm
    , fromOraDataType = oraIdentifier nm
    -- , oraDataTypeSerialized = domainType nm
    }

  charType prec charSet = OraDataTypeSyntax
    { oraDataTypeDescr = OraDataTypeDescr "CHAR" (fmap fromIntegral prec) Nothing Nothing
    , fromOraDataType = "CHAR" <> oraOptPrec prec <> oraOptCharSet charSet
    -- , oraDataTypeSerialized = charType prec charSet
    }

  varCharType prec charSet
    = OraDataTypeSyntax (OraDataTypeDescr "VARCHAR2" (fmap fromIntegral prec) Nothing Nothing)
                        ("VARCHAR2" <> oraOptPrec prec <> oraOptCharSet charSet)
                        -- (varCharType prec charSet)
  nationalCharType prec
    = OraDataTypeSyntax (OraDataTypeDescr "NCHAR" (fmap fromIntegral prec) Nothing Nothing)
                        ("NCHAR" <> oraOptPrec prec)
                        -- (nationalCharType prec)
  nationalVarCharType prec
    = OraDataTypeSyntax (OraDataTypeDescr "NVARCHAR2" (fmap fromIntegral prec) Nothing Nothing)
                        ("NVARCHAR2" <> oraOptPrec prec)
                        -- (nationalVarCharType prec)
  bitType _ = error "bitType: not implemented"
  varBitType _ = error "varBitType: not implemented"
  numericType = \case
    Nothing ->
      OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing Nothing Nothing)
                        ("NUMBER" <> oraOptNumericPrec Nothing)
                        -- (numericType Nothing)
    prec@(Just (p, s)) ->
      OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing (Just p) (fmap fromIntegral s))
                        ("NUMBER" <> oraOptNumericPrec prec)
                        -- (numericType prec)
  decimalType = numericType
  intType
    = OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing (Just 10) (Just 0))
                        ("NUMBER" <> oraOptNumericPrec (Just (10, Just 0)))
                        -- intType
  smallIntType
    = OraDataTypeSyntax (OraDataTypeDescr "NUMBER" Nothing (Just 5) (Just 0))
                        ("NUMBER" <> oraOptNumericPrec (Just (5, Just 0)))
                        -- smallIntType
  floatType _
    = OraDataTypeSyntax (OraDataTypeDescr "BINARY_FLOAT" Nothing Nothing Nothing)
                        "BINARY_FLOAT"
                        -- (floatType Nothing)
  doubleType
    = OraDataTypeSyntax (OraDataTypeDescr "BINARY_DOUBLE" Nothing Nothing Nothing)
                        "BINARY_DOUBLE"
                        -- doubleType
  realType = error "realType: not implemented"
  dateType
    = OraDataTypeSyntax (OraDataTypeDescr "DATE" Nothing Nothing Nothing)
                        "DATE"
                        -- dateType
  timeType _ _ = error "timeType: not implemented"
  timestampType prec withTz
    = OraDataTypeSyntax (OraDataTypeDescr "TIMESTAMP" Nothing Nothing Nothing)
                        ("TIMESTAMP" <> oraOptPrec prec <> if withTz then " WITH TIME ZONE" else mempty)
                        -- (timestampType prec withTz)

oraOptPrec :: Maybe Word -> OraSyntax
oraOptPrec Nothing = mempty
oraOptPrec (Just x) = oraParens $ emit $ fromString $ show x

oraOptCharSet :: Maybe T.Text -> OraSyntax
oraOptCharSet Nothing = mempty
oraOptCharSet (Just cs) = " CHARACTER SET " <> (emit $ BB.byteString $ TE.encodeUtf8 cs)

oraOptNumericPrec :: Maybe (Word, Maybe Word) -> OraSyntax
oraOptNumericPrec Nothing = mempty
oraOptNumericPrec (Just (prec, Nothing)) = oraOptPrec (Just prec)
oraOptNumericPrec (Just (prec, Just dec)) =
  oraParens
  ((emit $ fromString $ show prec) <> ", " <> (emit $ fromString $ show dec))

instance IsSql92ExtractFieldSyntax OraExtractFieldSyntax where
  secondsField = error "secondsField: not implemented"
  minutesField = error "minutesField: not implemented"
  hourField = error "hourField: not implemented"
  dayField = error "dayField: not implemented"
  monthField = error "monthField: not implemented"
  yearField = error "yearField: not implemented"

instance IsSql92TableSourceSyntax OraTableSourceSyntax where
  type Sql92TableSourceTableNameSyntax OraTableSourceSyntax = OraTableNameSyntax
  type Sql92TableSourceSelectSyntax OraTableSourceSyntax = OraSelectSyntax
  type Sql92TableSourceExpressionSyntax OraTableSourceSyntax = OraExpressionSyntax

  tableNamed :: OraTableNameSyntax -> OraTableSourceSyntax
  tableNamed = OraTableSourceSyntax . fromOraTableName

  tableFromSubSelect :: OraSelectSyntax -> OraTableSourceSyntax
  tableFromSubSelect = OraTableSourceSyntax . oraParens . fromOraSelect

  tableFromValues :: [[OraExpressionSyntax]] -> OraTableSourceSyntax
  tableFromValues vss =
    OraTableSourceSyntax . oraParens $
    "VALUES " <>
    (oraCommas $ map (oraParens . oraCommas . map fromOraExpression) vss)


instance IsSql92FieldNameSyntax OraFieldNameSyntax where
  qualifiedField :: Text -> Text -> OraFieldNameSyntax
  qualifiedField a b = OraFieldNameSyntax $ oraIdentifier a <> "." <> oraIdentifier b

  unqualifiedField :: Text -> OraFieldNameSyntax
  unqualifiedField a = OraFieldNameSyntax $ oraIdentifier a

instance HasSqlValueSyntax OraValueSyntax SqlNull where
  sqlValueSyntax _ = OraValueSyntax "NULL"

instance HasSqlValueSyntax OraValueSyntax Bool where
  sqlValueSyntax True  = OraValueSyntax "TRUE"
  sqlValueSyntax False = OraValueSyntax "FALSE"

instance HasSqlValueSyntax OraValueSyntax Double where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.doubleDec d)

instance HasSqlValueSyntax OraValueSyntax Float where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.floatDec d)

instance HasSqlValueSyntax OraValueSyntax Int where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.intDec d)

instance HasSqlValueSyntax OraValueSyntax Int8 where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.int8Dec d)

instance HasSqlValueSyntax OraValueSyntax Int16 where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.int16Dec d)

instance HasSqlValueSyntax OraValueSyntax Int32 where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.int32Dec d)

instance HasSqlValueSyntax OraValueSyntax Int64 where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.int64Dec d)

instance HasSqlValueSyntax OraValueSyntax Integer where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.integerDec d)

instance HasSqlValueSyntax OraValueSyntax Word where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.wordDec d)

instance HasSqlValueSyntax OraValueSyntax Word8 where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.word8Dec d)

instance HasSqlValueSyntax OraValueSyntax Word16 where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.word16Dec d)

instance HasSqlValueSyntax OraValueSyntax Word32 where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.word32Dec d)

instance HasSqlValueSyntax OraValueSyntax Word64 where
  sqlValueSyntax d = OraValueSyntax $ emit (BB.word64Dec d)

instance HasSqlValueSyntax OraValueSyntax Text where
  sqlValueSyntax t = OraValueSyntax $ emitValue $ Odpi.NativeBytes $ TE.encodeUtf8 t

instance HasSqlValueSyntax OraValueSyntax TL.Text where
  sqlValueSyntax = sqlValueSyntax . TL.toStrict

instance HasSqlValueSyntax OraValueSyntax [Char] where
  sqlValueSyntax = sqlValueSyntax . T.pack

instance HasSqlValueSyntax OraValueSyntax Scientific where
  sqlValueSyntax = OraValueSyntax . emit . scientificBuilder

instance HasSqlValueSyntax OraValueSyntax Day where
  sqlValueSyntax d = OraValueSyntax $ emit ("'" <> dayBuilder d <> "'")

dayBuilder :: Day -> Builder
dayBuilder d =
    BB.integerDec year <> "-" <>
    (if month < 10 then "0" else mempty) <> BB.intDec month <> "-" <>
    (if day   < 10 then "0" else mempty) <> BB.intDec day
  where
    (year, month, day) = Ti.toGregorian d

instance HasSqlValueSyntax OraValueSyntax TimeOfDay where
  sqlValueSyntax d = OraValueSyntax (emit ("'" <> todBuilder d <> "'"))

todBuilder :: TimeOfDay -> Builder
todBuilder d =
    (if Ti.todHour d < 10 then "0" else mempty) <> BB.intDec (Ti.todHour d) <> ":" <>
    (if Ti.todMin  d < 10 then "0" else mempty) <> BB.intDec (Ti.todMin  d) <> ":" <>
    (if secs6 < 10 then "0" else mempty) <> fromString (showFixed False secs6)
  where
    secs6 :: Fixed E6
    secs6 = fromRational $ toRational $ Ti.todSec d

emit :: Builder -> OraSyntax
emit b = OraSyntax (\_ -> pure b) mempty

emitValue :: Odpi.NativeValue -> OraSyntax
emitValue v = OraSyntax ($ v) (DL.singleton v)

-- | A best effort attempt to implement the escaping rules of Oracle. This is
-- never used to escape data sent to the database; only for emitting scripts or
-- displaying syntax to the user.
oraEscape :: Text -> T.Text
oraEscape = T.concatMap (\c -> if c == '"' then "\"\"" else T.singleton c)

oraIdentifier :: T.Text -> OraSyntax
oraIdentifier t = OraSyntax (\_ -> pure $ TE.encodeUtf8Builder $ oraEscape t) mempty
  

oraSepBy :: Monoid a => a -> [a] -> a
oraSepBy sep xs = mconcat $ intersperse sep xs

oraCommas :: (Monoid a, IsString a) => [a] -> a
oraCommas = oraSepBy ", "

oraSurroundWith :: Semigroup a => a -> a -> a -> a
oraSurroundWith open close x = open <> x <> close

oraParens :: (Semigroup a, IsString a) => a -> a
oraParens = oraSurroundWith "(" ")"

memptyOr :: Monoid b => (a -> b) -> Maybe a -> b
memptyOr f = maybe mempty f

