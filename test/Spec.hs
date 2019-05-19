{-# LANGUAGE OverloadedStrings #-}

module Main where

import Test.Hspec
import Test.QuickCheck
import Test.QuickCheck.Monadic

import Data.Int
import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as B8
import Data.Scientific
import Data.Text (Text)
import Data.Tuple.Only

import System.Environment

import qualified Database.Odpi as Odpi

import Database.Beam
import Database.Beam.Oracle

import Chinook.Schema

------------------------------------------------------------------------------

conf :: Odpi.OdpiConf
conf = Odpi.OdpiConf
  { Odpi._odpiConf_username = "user"
  , Odpi._odpiConf_password = "password"
  , Odpi._odpiConf_connstr  =  "host:port/dbName"
  }

--chinookDbChecked :: CheckedDatabaseSettings be ChinookDb
--chinookDbChecked = defaultMigratableDbSettings @OraCommandSyntax

main :: IO ()
main = do
  setEnv "NLS_LANG" "AMERICAN_AMERICA.AL32UTF8"
  withConn $ hspec . spec

spec :: Odpi.Connection -> Spec
spec conn = do
  describe "beam-oracle" $ do
    it "can run a query" $ do
      r <- runQ $ q0 1
      r `shouldBe` (Just "AC/DC")

  describe "data types" $ do
    describe "text" $ do
      it "handles UTF-8" $ do
        r <- runQ $ q0 146
        r `shouldBe` (Just "Titãs")

  describe "inner join" $ do
    it "passes unit tests" $ do
      r <- runQ joinInner1
      (length r) `shouldBe` (347 :: Int)

  describe "left outer join" $ do
    it "passes unit tests" $ do
      r <- runQ joinLeft1
      (length r) `shouldBe` (418 :: Int)

  describe "ordering" $ do
    it "works asc" $ do
      r <- runQ orderAsc
      (head r) `shouldBe` "A Cor Do Som"
    it "works desc" $ do
      r <- runQ orderDesc
      (head r) `shouldBe` "Zeca Pagodinho"

  describe "aggregates" $ do
    it "count" $ do
      r <- runQ aggCount1
      r `shouldBe` (Just 412)
    it "sum" $ do
      r <- runQ aggSum
      r `shouldBe` (Just 2328.6)
    it "min" $ do
      r <- runQ aggMin
      r `shouldBe` (Just 0.99)
    it "max" $ do
      r <- runQ aggMax
      r `shouldBe` (Just 25.86)
    it "avg, does not loose precision" $ do
      -- r1 <- runQ aggAvg
      -- r1 `shouldBe` (Just 393599.2121039109334855837853268626891236)
      r2 <- fmap (fromOnly . head) $ rawQ $ "select avg(milliseconds) from track"
      r2 `shouldBe` (393599.2121039109334855837853268626891236 :: Scientific)

  describe "aggregates with group by" $ do
    it "count" $ do
      r <- runQ agg2Count1
      r `shouldBe` [(CustomerId 44, 7), (CustomerId 59, 6)]
    it "sum" $ do
      r <- runQ agg2Sum
      r `shouldBe` [(CustomerId 44, 41.62), (CustomerId 59, 36.64)]

  describe "limit" $ do
    it "returns empty list when 0" $ do
      r <- runQ $ limit1 0
      r `shouldBe` []
    it "is a noop when >= number of rows" $ do
      r <- runQ $ limit1 300
      (length r) `shouldBe` 275
    it "passes unit tests" $ do
      r1 <- runQ $ limit1 1
      r1 `shouldBe` ["A Cor Do Som"]
      r2 <- runQ $ limit1 2
      r2 `shouldBe` ["A Cor Do Som", "AC/DC"]
    it "passes property tests" $ property $ \l -> l > 0 && l < 275 ==> monadicIO $ do
        (r1, r2) <- run $ do
          r1 <- runQ' conn $ limit1 l
          r2 <- fmap (map fromOnly) $ rawQ' conn $ B8.unwords
            [ "select * from ("
            ,   "select name from artist order by name"
            , ") where rownum <= ", B8.pack (show l)
            ]
          pure (r1, r2)
        assert $ r1 == r2

  describe "offset" $ do
    it "is a noop when 0" $ do
      r <- runQ $ offset1 0
      (length r) `shouldBe` 275
    it "returns empty list when >= number of rows" $ do
      r <- runQ $ offset1 300
      r `shouldBe` []
    it "passes unit tests" $ do
      r <- runQ $ offset1 274
      r `shouldBe` ["Zeca Pagodinho"]
    it "passes property tests" $ property $ \o -> o >= 0 && o < 275 ==> monadicIO $ do
        (r1, r2) <- run $ do
          r1 <- runQ' conn $ offset1 o
          r2 <- fmap (map fromOnly) $ rawQ' conn $ B8.unwords
            [ "select name from ("
            ,   "select a.name as name, rownum as rnum"
            ,   "from (select name from artist order by name) a"
            , ") where rnum >", B8.pack (show o)
            ]
          pure (r1, r2)
        assert $ r1 == r2

  describe "offset and limit" $ do
    it "work well together" $ do
      r1 <- runQ $ offsetLimit1 0 0
      r1 `shouldBe` []
      r2 <- runQ $ offsetLimit1 1 1
      r2 `shouldBe` ["AC/DC"]
      r3 <- runQ $ offsetLimit1 2 2
      r3 `shouldBe` ["Aaron Copland & London Symphony Orchestra", "Aaron Goldberg"]

  describe "filtering" $ do
    it "handles simple injection" $ do
      r <- runQ $ runSelectReturningList $ select $ do
        c <- all_ $ customer chinookDb
        guard_ $ customerFirstName c ==. val_ "Bobby Tables') or 1=1 -- "
        pure c
      (length r) `shouldBe` 0

-- hsSchema :: Connection -> IO ()
-- hsSchema conn = do
--   s <- runBeamOracle conn (haskellSchema migrationBackend)
--   withFile "NewBeamSchema.hs" WriteMode $ \h -> do
--     hSetBuffering h LineBuffering
--     hPutStrLn h s

withConn :: (Odpi.Connection -> IO a) -> IO a
withConn f =
  Odpi.withContext $ \cxt ->
    Odpi.withConnection cxt conf f

runQ :: Ora a -> IO a
runQ q = withConn $ \conn -> runQ' conn q

runQ' :: Odpi.Connection -> Ora a -> IO a
runQ' = runBeamOracleDebug putStrLn

rawQ :: Odpi.FromRow a => ByteString -> IO [a]
rawQ q = withConn $ \conn -> rawQ' conn q

rawQ' :: Odpi.FromRow a => Odpi.Connection -> ByteString -> IO [a]
rawQ' = Odpi.querySimple

q0 :: Int32 -> Ora (Maybe Text)
q0 aid = runSelectReturningOne $ select $ do
  a <- all_ $ artist chinookDb
  guard_ $ pk a ==. val_ (ArtistId aid)
  pure $ artistName a

joinInner1 :: Ora [(Artist, Album)]
joinInner1 = runSelectReturningList $ select $ do
  a <- all_ $ artist chinookDb
  al <- join_ (album chinookDb) $ \r -> albumArtist r ==. pk a
  pure (a, al)

joinLeft1 :: Ora [(Artist, Maybe Album)]
joinLeft1 = runSelectReturningList $ select $ do
  a <- all_ $ artist chinookDb
  al <- leftJoin_ (all_ $ album chinookDb) $ \r -> albumArtist r ==. pk a
  pure (a, al)

orderAsc :: Ora [Text]
orderAsc = runSelectReturningList $ select $ orderBy_ asc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a

orderDesc :: Ora [Text]
orderDesc = runSelectReturningList $ select $ orderBy_ desc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a

aggCount1 :: Ora (Maybe Int)
aggCount1 = runSelectReturningOne $ select $
  aggregate_ (\_ -> countAll_) $
  all_ $ invoice chinookDb

aggSum :: Ora (Maybe Scientific)
aggSum = runSelectReturningOne $ select $
  aggregate_ (\i -> fromMaybe_ 0 $ sum_ (invoiceTotal i)) $
  all_ $ invoice chinookDb

aggMin :: Ora (Maybe Scientific)
aggMin = runSelectReturningOne $ select $
  aggregate_ (\i -> fromMaybe_ 0 $ min_ (invoiceTotal i)) $
  all_ $ invoice chinookDb

aggMax :: Ora (Maybe Scientific)
aggMax = runSelectReturningOne $ select $
  aggregate_ (\i -> fromMaybe_ 0 $ max_ (invoiceTotal i)) $
  all_ $ invoice chinookDb

-- aggAvg :: Ora (Maybe Scientific)
-- aggAvg = runSelectReturningOne $ select $
--   aggregate_ (\t -> fromMaybe_ 0 $ avg_ (cast_ (trackMilliseconds t) numericType)) $
--   all_ $ track chinookDb

agg2Count1 :: Ora [(CustomerId, Int)]
agg2Count1 = runSelectReturningList $ select $
  aggregate_ (\c -> (group_ c, countAll_)) $ do
    i <- all_ $ invoice chinookDb
    guard_ $ invoiceCustomer i ==. CustomerId (val_ 44) ||. invoiceCustomer i ==. CustomerId (val_ 59)
    pure (invoiceCustomer i)

agg2Sum :: Ora [(CustomerId, Scientific)]
agg2Sum = runSelectReturningList $ select $
  aggregate_ (\i -> (group_ (invoiceCustomer i), fromMaybe_ 0 $ sum_ (invoiceTotal i))) $
  filter_ (\i -> invoiceCustomer i ==. CustomerId (val_ 44) ||. invoiceCustomer i ==. CustomerId (val_ 59)) $
  all_ $ invoice chinookDb

limit1 :: Integer -> Ora [Text]
limit1 l = runSelectReturningList $ select $ limit_ l $ orderBy_ asc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a

offset1 :: Integer -> Ora [Text]
offset1 o = runSelectReturningList $ select $ offset_ o $ orderBy_ asc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a

offsetLimit1 :: Integer -> Integer -> Ora [Text]
offsetLimit1 o l = runSelectReturningList $ select $ limit_ l $ offset_ o $ orderBy_ asc_ $ do
  a <- all_ $ artist chinookDb
  pure $ artistName a
