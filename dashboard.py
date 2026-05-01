import streamlit as st
import logging
import json
from datetime import datetime
from pathlib import Path
import main

logger = logging.getLogger(__name__)

@st.cache_data
def read_json(run_id):
    with open(f"outputs/json/results_{run_id}.json", 'r', encoding='utf-8') as file:
        return json.load(file)

st.set_page_config(page_title="📊 Auto Schema Pipeline",layout="wide")

uploaded_file=st.file_uploader(label="Upload a CSV file",type=".csv")

if uploaded_file:
    base_name=Path(uploaded_file.name).stem
    time=datetime.now().strftime("%Y%m%d_%H%M%S")
    final_file_name=base_name+"_"+time+".csv"
    path=f"data/raw/{final_file_name}"
    
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path,"wb") as f:
        f.write(uploaded_file.getbuffer())
    
    with st.spinner("Analyzing server logs and generating schema...",show_time=True):
        run_id,error_message = main.run_pipeline(path)
    
    if run_id:
        st.success("Pipeline run successfull")
        output_data = read_json(run_id)
        #rest logic to be added
    else:
        st.error(f"Pipeline failed to run! Error : {error_message}")