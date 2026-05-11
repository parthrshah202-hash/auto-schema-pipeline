import pytest
from src.transform import clean_data
import pandas as pd

data={"col1":[1,4,7,1],"col2":[2,None,8,2],"col3":[3,6,9,3]}
df=pd.DataFrame(data=data)

clean_df,total_rows,missing_values,duplicate_rows_dropped=clean_data(df)

def test_missing_values():
    """Checks the correctness of clean_data function to handle missing values
    """
    assert missing_values==1
    
def test_total_rows():
    """Checks the correctness of clean_data function to return total initial number of rows
    """
    assert total_rows==len(df)

def test_duplicates_dropped():
    """Checks the correctness of clean_data function to handle the number of duplicates dropped
    """
    assert len(clean_df)==len(df)-1
    
def test_cleaned_df():
    """Checks the correctness of clean_data function to handle the correct cleaned dataframe
    """
    correct_data={"col1":[1,4,7],"col2":[2,2,8],"col3":[3,6,9]}
    correct_df=pd.DataFrame(data=correct_data)
    clean_df.reset_index(drop=True, inplace=True)
    correct_df['col2']=correct_df['col2'].astype('float64')
    pd.testing.assert_frame_equal(correct_df, clean_df)