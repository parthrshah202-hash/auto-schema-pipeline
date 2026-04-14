import sys
sys.path.append("src")
import logging
from datetime import datetime
import ingestion
import transform
import load
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
    raw_data,file_name,file_size = ingestion.load_data("data/raw/server_logs.csv")
    cleaned_data,total_rows,missing_values,duplicate_rows_dropped = transform.clean_data(raw_data)
    end_time=datetime.now()
    duration=int((end_time-start_time).total_seconds())
    status="Success"
    run_id = load.insert_pipeline_runs(start_time,file_name,file_size,duration,status,trigger)
    error=None
    load.insert_validate_result(run_id,total_rows,missing_values,duplicate_rows_dropped,error)
    logger.info("Pipeline runs Successfully")
except Exception as e:
    status="Failed"
    file_name=None
    file_size=None
    duration = int((datetime.now() - start_time).total_seconds())
    run_id = load.insert_pipeline_runs(start_time,file_name,file_size,duration,status,trigger)
    logger.error(f"Pipeline Failed to run : {e}")
    