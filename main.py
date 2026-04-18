import sys
sys.path.append("src")
import logging
from datetime import datetime
from pathlib import Path
import ingestion
import transform
import load
import schema_detector
import analysis


logging.basicConfig(
    filename="logs/pipeline.log",
    format='%(asctime)s %(levelname)s: %(message)s',
    filemode='a'
)

logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

start_time=datetime.now()
trigger="Manual"

try:
    engine=load.get_connection()
    
    load.set_up_tables(engine)
    
    raw_data,file_name,file_size = ingestion.load_data("data/raw/server_logs.csv")
    
    cleaned_data,total_rows,missing_values,duplicate_rows_dropped = transform.clean_data(raw_data)
    
    schema_dict=schema_detector.detect_schema(cleaned_data)
    
    table_name=file_name
    
    load.create_table(table_name,schema_dict,engine)
    
    load.insert_data(table_name,cleaned_data,engine)
    
    end_time=datetime.now()
    duration=int((end_time-start_time).total_seconds())
    status="Success"
    
    run_id = load.insert_pipeline_runs(start_time,file_name,file_size,duration,status,trigger,engine)
    
    error=None
    
    load.insert_validate_result(run_id,total_rows,missing_values,duplicate_rows_dropped,error,engine)
    
    logger.info("Pipeline runs Successfully")
    
except Exception as e:
    status="Failed"
    file_name=None
    file_size=None
    duration = int((datetime.now() - start_time).total_seconds())
    run_id = load.insert_pipeline_runs(start_time,file_name,file_size,duration,status,trigger,engine)
    logger.error(f"Pipeline Failed to run : {e}")
    