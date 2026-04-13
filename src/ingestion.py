import pandas as pd
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)


def load_data(filepath):
    """
    Loads a CSV file and returns it as a pandas DataFrame along with filename and filesize
    
    Reads data from the given file path for further processing.
    
    Args:
        filepath (str): Path to the CSV file.
    
    Returns:
        pandas.DataFrame: Loaded dataset.
    """
    try:
        df=pd.read_csv(filepath,skip_blank_lines=True)
        logger.info(f"{filepath} file has been read successfully")
        file_name=Path(filepath).name
        file_size=os.path.getsize(filepath)
        return df, file_name, file_size
    
    except FileNotFoundError:
        logger.error("Error 404 : File Not Found !")
        exit(1)
        
    