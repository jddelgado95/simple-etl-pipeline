#Purpose:
#Cleans or processes the extracted data.
#What it does:
#Example tasks: remove nulls, convert datatypes, calculate new columns, normalize names/salaries.
#Transforms the raw data into a usable format for downstream systems.
#Returns the cleaned/processed data.

import pandas as pd

#Defines a function named transform.
#Takes a pandas DataFrame df as input and returns another DataFrame
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform the data - e.g., filter and add new columns."""
    # Example transformation: filter only Engineering department
    #Filters the rows to only include employees in the Engineering department.
    #df['department'] == 'Engineering' creates a boolean mask.
    #.copy() is used to avoid modifying the original DataFrame slice.
    df = df[df['department'] == 'Engineering'].copy()
    
    # Add a new column, e.g., salary after 10% bonus
    #Adds a new column named salary_bonus.
    #Each employee’s salary is increased by 10% (multiplied by 1.10
    df['salary_bonus'] = df['salary'] * 1.10
    
    return df