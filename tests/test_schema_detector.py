import pytest
from src.schema_detector import detect_schema
import pandas as pd

data={"col1":[1,2,3],"col2":[1.5,5.5,6.9],"col3":["parth","aakash","saumik"],"col4":[True,True,False]}
df=pd.DataFrame(data=data)

def test_mapping():
    schema_dict=detect_schema(df)
    condition=schema_dict['col1']=="INTEGER" and schema_dict['col2']=="FLOAT" and schema_dict['col3']=="TEXT" and schema_dict['col4']=="BOOLEAN"
    assert condition