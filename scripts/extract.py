import pandas as pd

def extract(file_path: str) -> pd.DataFrame:
    """Extract data from CSV."""
    df = pd.read_csv(file_path)
    return df