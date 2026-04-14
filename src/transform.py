import pandas as pd
import logging

logger = logging.getLogger(__name__)

def clean_data(df):
    """
    Cleans the dataset by handling missing and invalid values.
    
    Removes duplicates and filters out unrealistic or incomplete records.
    
    Args:
        df (pandas.DataFrame): Raw dataset to clean.
    
    Returns:
        pandas.DataFrame: Cleaned dataset ready for analysis.
    """
    total_rows=len(df)
    
    #Filling the missing data
    missing_values_col=df.isnull().sum().to_dict()
    missing_values=sum(missing_values_col.values())
    
    numeric_cols = df.select_dtypes(include='number').columns
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())
    
    text_cols = df.select_dtypes(include='object').columns
    df[text_cols] = df[text_cols].fillna("unknown")
    
    logger.info("Filled the Missing values")
    
    #Dropping duplicates
    duplicate_rows_dropped=df.duplicated().sum()
    df=df.drop_duplicates()
    logger.info("Dropped Duplicates")
    
    
    
    logger.info("Data Cleaned")
    return df,int(total_rows),int(missing_values),int(duplicate_rows_dropped)
