import pandas as pd
import logging

logger = logging.getLogger(__name__)

def detect_schema(df):
    """Infer the SQL schema mapping from a pandas DataFrame.
    
    Determine the type of data in each column of pandas dataframe and maps it to SQL database's table
    
    Args:
        df(DataFrame): The input dataset
        
    Returns:
        dict: A dictionary where keys are column names and values are the equivalent SQL data types.
    """
    map_dict={'int64':'INTEGER','float64':'FLOAT','object':'TEXT','datetime64[ns]':'TIMESTAMP','bool':'BOOLEAN'}
    schema_dict={}
    for col in df.columns:
        pandas_type=str(df[col].dtype)
        schema_dict[col] = map_dict.get(pandas_type, 'TEXT')
            
    logger.info("Schema detection successfull")
    return schema_dict
    