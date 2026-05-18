import streamlit as st
import os

try:
    os.environ["DB_HOST"] = st.secrets["DB_HOST"]
    os.environ["DB_PORT"] = st.secrets["DB_PORT"]
    os.environ["DB_NAME"] = st.secrets["DB_NAME"]
    os.environ["DB_USER"] = st.secrets["DB_USER"]
    os.environ["DB_PASSWORD"] = st.secrets["DB_PASSWORD"]
    os.environ["GEMINI_API_KEY"] = st.secrets["GEMINI_API_KEY"]
except Exception:
    pass  # Running locally, python-dotenv handles it

import logging
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from src.visualise import get_slug
import main
import load

logger = logging.getLogger(__name__)

@st.cache_data
def read_json(run_id):
    """Load and deserialize the JSON results for a specific pipeline execution.
    
    Args:
        run_id (int): The unique identifier for the current pipeline execution, used to locate specific JSON file

    Returns:
        dict: The entire JSON file is transformed into a dictionary
    """
    with open(f"outputs/json/results_{run_id}.json", 'r', encoding='utf-8') as file:
        return json.load(file)
    
def show_landing_page():
    """"Render the landing page content for the dashboard
    
    It loads the landing page of dashboard showing project information, tech-stack used, pipeline run history and who built it.
    """
    #Project info
    st.title("📊 Auto Schema Pipeline")
    st.header("Drop any CSV. The pipeline figures out the rest.")
    st.subheader("Description : A production-grade data engineering pipeline that ingests any CSV, auto-detects its schema, stores it in PostgreSQL, and uses Google Gemini to dynamically generate and execute meaningful analysis queries — with zero hardcoding.")
    st.divider()
    st.write("")
        
    st.subheader("🎯Project Flow")
    st.write("Designed using modular Python scripts to simulate a real-world ETL pipeline architecture.")
    st.write("📂 CSV Upload ➡️ 🔍 Schema Detection ➡️ ✅Validation ➡️🗄️ PostgreSQL ➡️ 🤖 Gemini Analysis ➡️ 📊Dashboard ➡️ 📄 PDFReport")
    st.divider()
    st.write("")
            
    st.subheader("🛠️ TechStack")
    col1,col2,col3,col4,col5=st.columns(5)
    with col1:
        st.markdown("### Database")
        st.write("PostgreSQL")
    with col2:
        st.markdown("### Engine")
        st.write("Python")
        st.write("Pandas")
    with col3:
        st.markdown("### AI")
        st.write("Google Gemini")
    with col4:
        st.markdown("### Visuals")
        st.write("Matplotlib")
        st.write("Streamlit")
    with col5:
        st.markdown("### PDF")
        st.write("FPDF")
        
    st.subheader("Pipeline Run History")
    try:
        engine=load.get_connection()
        pipeline_history=load.get_run_history(engine)
        pipeline_history_df=pd.DataFrame(pipeline_history)
        pipeline_history_df['file_size']=pipeline_history_df['file_size']/1024/1024
        pipeline_history_df=pipeline_history_df.rename(columns={'file_size':'file_size (MB)'})
        st.dataframe(pipeline_history_df)
        st.write("")
        st.info("This indicates that Live DB is present")
        st.divider()
    except Exception as e:
        st.info("Please upload a CSV to see run history")
                
    st.write("")
    st.info("👆 Upload a CSV file at the top to get started")
            
    st.info("Made by PARTH SHAH to explore Data Engineering")
    

st.set_page_config(page_title="📊 Auto Schema Pipeline",layout="wide")

if 'run_id' not in st.session_state:
    uploaded_file=st.file_uploader(label="Upload a CSV file",type=".csv")
    #Project info
    show_landing_page()
    
    if uploaded_file:
        base_name=Path(uploaded_file.name).stem
        time=datetime.now().strftime("%Y%m%d_%H%M%S")
        final_file_name=base_name+"_"+time+".csv"
        path=f"data/raw/{final_file_name}"
        
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path,"wb") as f:
            f.write(uploaded_file.getbuffer())
        
        with st.spinner("Analyzing CSV and generating schema...",show_time=True):
            run_id,error_message = main.run_pipeline(path)
            
        if run_id:
            st.session_state['run_id'] = run_id
            output_data = read_json(run_id)
            st.session_state['output_data']=output_data
            st.session_state['uploaded_filename'] = uploaded_file.name
            st.success("Pipeline run successfull")
            st.rerun()
        else:
            st.error(f"Pipeline failed to run! Error : {error_message}")
           
else:
    page="Overview"
    with st.sidebar:
        st.title("Navigation")
        st.write("")
        page=st.selectbox("Choose a page",["Home","Overview","Analysis Result","Discarded Queries"])
            
        pdf_report_path=f"outputs/reports/analysis_report_{st.session_state['run_id']}.pdf"
        try:
            with open(pdf_report_path,"rb") as f:
                report=f.read()
            st.download_button(label="⬇️ Download Analysis",data=report,file_name="Analysis Report.pdf",mime="application/pdf")
        except Exception as e:
            st.warning(f"PDF report not available : {e}")
                
    if page=="Home":
        uploaded_file=st.file_uploader(label="Upload a CSV file",type=".csv")
        
        if uploaded_file and uploaded_file.name != st.session_state['uploaded_filename']:
            del st.session_state['run_id']
            del st.session_state['output_data']
            del st.session_state['uploaded_filename']
            st.rerun()
        else:
            #Project info
           show_landing_page()
        
    elif page=="Overview":
        #file_data
        file_name=st.session_state.output_data["file_data"]["file_name"]
        file_size=st.session_state.output_data["file_data"]["file_size"]
        file_inserted_time=st.session_state.output_data["file_data"]["file_inserted"]
        
        col1,col2,col3 = st.columns(3)
        
        st.subheader("📁 File Details")
        st.write("")
        col1.metric("File Name",file_name)
        col2.metric("File Size",f"{file_size} MB")
        col3.metric("Time Inserted",file_inserted_time)
        
        st.divider()
        
        #file_info
        st.subheader("📊 Pipeline Summary")
        st.write("")
        total_rows=st.session_state.output_data["file_info"]["total_rows"]
        total_columns=st.session_state.output_data["file_info"]["total_columns"]
        missing_values=st.session_state.output_data["file_info"]["missing_values"]
        duplicate_rows_dropped=st.session_state.output_data["file_info"]["duplicate_rows_dropped"]
        
        a,b=st.columns(2)
        c,d=st.columns(2)
        
        a.metric("Total Rows",total_rows,border=True)
        b.metric("Total Columns",total_columns,border=True)
        c.metric("Total Missing Values",missing_values,border=True)
        d.metric("Total Duplicates Dropped",duplicate_rows_dropped,border=True)
        
    elif page=="Analysis Result":
        st.subheader("Analysis Results")
        analysis_result=st.session_state.output_data["analysis_result"]
        #This analysis result is a dict where key = question and value = answer
        if analysis_result:
            for question,answer in analysis_result.items():
                st.markdown(f"**Q. {question}**")
                st.dataframe(answer)
                st.write("")
                slug=get_slug(question)
                chart_path=Path(f"outputs/graphs/{st.session_state['run_id']}_{slug}.png")
                if chart_path.exists():
                    st.image(chart_path,)
                    st.write("")
                st.divider()
        else:
            st.warning("No analysis was obtained")
            
    else:
        st.subheader("Discarded Queries")
        discarded_queries=st.session_state.output_data["discarded_queries"]
        if discarded_queries:
            for question,value in discarded_queries.items():
                st.markdown(f"**Q. {question}**")
                col1,col2=st.columns(2)
                with col1:
                    st.code(value['query'],language='sql')
                with col2:
                    st.write(value['reason'])
                st.divider()
        else:
            st.info("All queries returned by AI were executed")      
        