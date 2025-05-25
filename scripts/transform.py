import pandas as pd

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the data - e.g., filter and add new columns."""
    # Example transformation: filter only Engineering department
    df = df[df['department'] == 'Engineering'].copy()
    
    # Add a new column, e.g., salary after 10% bonus
    df['salary_bonus'] = df['salary'] * 1.10
    
    return df