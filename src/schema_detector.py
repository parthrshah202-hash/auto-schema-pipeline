import pandas as pd
import logging

logger = logging.getLogger(__name__)

def detect_schema(df):
    try:
        map_dict={'int64':'INTEGER','float64':'FLOAT','object':'TEXT','datetime64':'TIMESTAMP','bool':'BOOLEAN'}
        schema_dict={}
        for col in df.columns:
            pandas_type=str(df[col].dtype)
            schema_dict[col] = map_dict.get(pandas_type, 'TEXT')
            
        logger.info("Schema detection successfull")
        return schema_dict
    except Exception as e:
        logger.error(f"Schema detection failed : {e}")