
# Azure SQL + CSV Ingestion Script
# See README in header for env vars and usage.
import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

def get_engine():
    server   = os.environ.get("AZ_SQL_SERVER")
    database = os.environ.get("AZ_SQL_DB")
    username = os.environ.get("AZ_SQL_USER")
    password = os.environ.get("AZ_SQL_PASSWORD")
    if not all([server, database, username, password]):
        raise RuntimeError("Missing one or more required env vars: AZ_SQL_SERVER, AZ_SQL_DB, AZ_SQL_USER, AZ_SQL_PASSWORD")
    driver = "ODBC Driver 18 for SQL Server"
    conn = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver.replace(' ', '+')}&TrustServerCertificate=yes"
    return sa.create_engine(conn, fast_executemany=True)

SCHEMA_SQL = r"""
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[BrandDetail]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[BrandDetail] (
        BrandID        INT            NOT NULL PRIMARY KEY,
        BrandName      NVARCHAR(255)  NULL,
        URL            NVARCHAR(512)  NULL,
        Category       NVARCHAR(255)  NULL,
        Subcategory    NVARCHAR(255)  NULL
    );
END;

IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[DailySpend]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[DailySpend] (
        [Date]        DATE           NOT NULL,
        BrandID       INT            NOT NULL,
        [State]       NVARCHAR(50)   NOT NULL,
        Spend         DECIMAL(18,2)  NULL,
        Transactions  INT            NULL,
        CONSTRAINT PK_DailySpend PRIMARY KEY ([Date], BrandID, [State]),
        CONSTRAINT FK_DailySpend_BrandDetail FOREIGN KEY (BrandID) REFERENCES [dbo].[BrandDetail](BrandID)
    );
END;
"""

MERGE_BRAND_SQL = r"""
MERGE [dbo].[BrandDetail] AS target
USING (SELECT DISTINCT BrandID, BrandName, URL, Category, Subcategory FROM #BrandStage) AS src
ON (target.BrandID = src.BrandID)
WHEN MATCHED THEN
    UPDATE SET BrandName = src.BrandName,
               URL = src.URL,
               Category = src.Category,
               Subcategory = src.Subcategory
WHEN NOT MATCHED BY TARGET THEN
    INSERT (BrandID, BrandName, URL, Category, Subcategory)
    VALUES (src.BrandID, src.BrandName, src.URL, src.Category, src.Subcategory);
"""

MERGE_DAILY_SQL = r"""
MERGE [dbo].[DailySpend] AS target
USING (
    SELECT [Date], BrandID, [State], Spend, Transactions FROM #DailyStage
) AS src
ON (target.[Date] = src.[Date] AND target.BrandID = src.BrandID AND target.[State] = src.[State])
WHEN MATCHED THEN
    UPDATE SET Spend = src.Spend,
               Transactions = src.Transactions
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([Date], BrandID, [State], Spend, Transactions)
    VALUES (src.[Date], src.BrandID, src.[State], src.Spend, src.Transactions);
"""

def infer_cols(df, candidates):
    lower = {c.lower(): c for c in df.columns}
    for options in candidates:
        for name in options:
            if name in lower:
                yield lower[name]
                break
        else:
            raise KeyError(f"Missing required column among: {options}")

def load_brand(engine, path):
    df = pd.read_csv(path)
    # (BrandID, BrandName, URL, Category, Subcategory)
    needed = [
        ("brandid","brand_id","id"),
        ("brandname","brand_name","brand"),
        ("url","website","homepage"),
        ("category","sector"),
        ("subcategory","sub_category","subsector")
    ]
    cols = list(infer_cols(df, needed))
    df = df.rename(columns=dict(zip(cols, ["BrandID","BrandName","URL","Category","Subcategory"])))[["BrandID","BrandName","URL","Category","Subcategory"]]
    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('tempdb..#BrandStage') IS NOT NULL DROP TABLE #BrandStage; CREATE TABLE #BrandStage (BrandID INT, BrandName NVARCHAR(255), URL NVARCHAR(512), Category NVARCHAR(255), Subcategory NVARCHAR(255));"))
        df.to_sql("#BrandStage", con=conn.connection, if_exists="append", index=False)
        conn.execute(text(MERGE_BRAND_SQL))

def load_daily(engine, path):
    df = pd.read_csv(path)
    # (Date, BrandID, State, Spend, Transactions)
    needed = [
        ("date","day"),
        ("brandid","brand_id","id"),
        ("state","us_state"),
        ("spend","amount"),
        ("transactions","txns","count")
    ]
    cols = list(infer_cols(df, needed))
    df = df.rename(columns=dict(zip(cols, ["Date","BrandID","State","Spend","Transactions"])))[["Date","BrandID","State","Spend","Transactions"]]
    df["Date"] = pd.to_datetime(df["Date"]).dt.date
    with engine.begin() as conn:
        conn.execute(text("IF OBJECT_ID('tempdb..#DailyStage') IS NOT NULL DROP TABLE #DailyStage; CREATE TABLE #DailyStage ([Date] DATE, BrandID INT, [State] NVARCHAR(50), Spend DECIMAL(18,2), Transactions INT);"))
        df.to_sql("#DailyStage", con=conn.connection, if_exists="append", index=False)
        conn.execute(text(MERGE_DAILY_SQL))

def main():
    data_dir = os.path.join(os.getcwd(), "data")
    brand_path = os.environ.get("BRAND_CSV", os.path.join(data_dir, "brand-detail-url-etc_0_0_0.csv"))
    daily_path = os.environ.get("DAILY_CSV", os.path.join(data_dir, "2021-01-19--data_01be88c2-0306-48b3-0042-fa0703282ad6_1304_5_0.csv"))
    if not os.path.exists(brand_path):
        raise FileNotFoundError(f"Brand CSV not found at: {brand_path}")
    if not os.path.exists(daily_path):
        raise FileNotFoundError(f"Daily CSV not found at: {daily_path}")
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(SCHEMA_SQL))
    load_brand(eng, brand_path)
    load_daily(eng, daily_path)
    print("Ingestion completed.")

if __name__ == "__main__":
    main()
