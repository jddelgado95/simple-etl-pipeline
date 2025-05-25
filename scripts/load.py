import sqlite3
import pandas as pd

def load(df: pd.DataFrame, db_path: str, table_name: str):
    """Load data into SQLite database."""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()