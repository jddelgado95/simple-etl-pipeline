#Purpose:
#Loads the transformed data into a target destination.
#Common targets:
# - Database (PostgreSQL, MySQL)
# - Cloud storage (S3, GCS)
# - Another CSV or Parquet file

#SQLite is a lightweight, file-based database—great for testing and small projects.
import sqlite3
import pandas as pd

def load(df: pd.DataFrame, db_path: str, table_name: str):
    #Load data into SQLite database."""
    #Creates a connection to the SQLite database at the path db_path. If the file doesn't exist, it creates a new one
    conn = sqlite3.connect(db_path)
    #Loads the DataFrame into the SQLite database as a table.
    """
    Parameters:
    table_name: Name of the table to write to.
    conn: The connection object used to write to the database.
    if_exists='replace':
    If the table already exists, it deletes it and creates a new one.
    Other options: 'fail', 'append'.
    index=False:
    Tells pandas not to include the DataFrame’s index as a column in the table
    """
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()