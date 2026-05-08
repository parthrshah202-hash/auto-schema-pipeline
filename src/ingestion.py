import pandas as pd
from pathlib import Path
import logging
import os

logger = logging.getLogger(__name__)


def load_data(filepath):
    """Load a CSV file and return it as a pandas DataFrame along with filename and filesize
    
    Reads data from the given file path for further processing.
    
    Args:
        filepath (str): Path to the CSV file.
    
    Returns:
        tuple: A tuple containing:
            - pandas.DataFrame: The loaded dataset.
            - str: The extracted filename without the extension.
            - int: The size of the file in bytes.
    
    Raises:
        FileNotFoundError: If file is not present at filepath
    """
    try:
        df=pd.read_csv(filepath)
        logger.info(f"{filepath} file has been read successfully")
        file_name=Path(filepath).stem
        file_size=os.path.getsize(filepath)
        return df, file_name, file_size
    
    except FileNotFoundError:
        logger.error(f"Error 404 : File Not Found at {filepath}!")
        raise
        
    