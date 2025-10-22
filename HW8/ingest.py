import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ----------  CONFIG  ----------
from pathlib import Path  # add this import (near the other imports)

# ----------  CONFIG  ----------
SERVER   = "datamanagement-server.database.windows.net"
DATABASE = "datamanagementdb"
USERNAME = "abhirami"
PASSWORD = "Dpword1289!"

BASE_DIR  = Path(__file__).parent
BRAND_CSV = str((BASE_DIR / "data" / "brand-detail-url-etc_0_0_0.csv").resolve())
SPEND_CSV = str((BASE_DIR / "data" / "2021-01-19--data_01be88c2-0306-48b3-0042-fa0703282ad6_1304_5_0.csv").resolve())
# ------------------------------

# ------------------------------

def build_engine():
    odbc_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    )
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": odbc_str})
    return create_engine(connection_url, fast_executemany=True)

def main():
    print("[INFO] Reading CSV files...")
    brand_df = pd.read_csv(BRAND_CSV)
    spend_df = pd.read_csv(SPEND_CSV)

    brand_df.columns = [c.lower() for c in brand_df.columns]
    spend_df.columns = [c.lower() for c in spend_df.columns]

    print("[INFO] Connecting to Azure SQL...")
    eng = build_engine()

    print("[INFO] Writing brand table...")
    brand_df.to_sql("brand", eng, schema="dbo", if_exists="replace", index=False)

    print("[INFO] Writing daily_spend table...")
    spend_df.to_sql("daily_spend", eng, schema="dbo", if_exists="replace", index=False)

    with eng.connect() as conn:
        for t in ("brand", "daily_spend"):
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM dbo.{t}")).scalar_one()
            print(f"[OK] {t}: {cnt} rows")

    print("[DONE] Ingestion complete.")

if __name__ == "__main__":
    main()
