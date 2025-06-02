#Purpose:
#Reads the raw data from employees.csv.
#Concepts:
#Uses libraries like pandas or csv to read structured data.
#Prepares the data for transformation.
#Might return a DataFrame or write it to a temporary file.

import pandas as pd

def extract(file_path: str) -> pd.DataFrame:
    """Extract data from CSV."""
    df = pd.read_csv(file_path)
    return df