import sys
sys.path.append("src")
import logging
from datetime import datetime
from pathlib import Path
import ingestion
import transform
import load
import schema_detector
import gemini
import validate_query
import execute_query
import visualise
import json


logging.basicConfig(
    filename="logs/pipeline.log",
    format='%(asctime)s %(levelname)s: %(message)s',
    filemode='a'
)

logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

start_time=datetime.now()
trigger="Manual"

engine=None
try:
    engine=load.get_connection()
    load.set_up_tables(engine)
    raw_data,file_name,file_size = ingestion.load_data("data/raw/server_logs.csv")
    cleaned_data,total_rows,missing_values,duplicate_rows_dropped = transform.clean_data(raw_data)
    
    schema_dict=schema_detector.detect_schema(cleaned_data)
    
    table_name=file_name
    
    load.create_table(table_name,schema_dict,engine)
    load.insert_data(table_name,cleaned_data,engine)
    
    prompt=gemini.build_prompt(cleaned_data,file_name,table_name,schema_dict,total_rows,duplicate_rows_dropped)
    analysis_dict=gemini.get_analysis(prompt)
    valid_queries,discarded_queries=validate_query.validate(analysis_dict,table_name,schema_dict)
    analysis_result=execute_query.execute_query(valid_queries,engine)
    if not analysis_result:
        logger.warning("No valid queries were executed. Analysis result is empty.")
    
    end_time=datetime.now()
    duration=int((end_time-start_time).total_seconds())
    status="Success"
        
    run_id = load.insert_pipeline_runs(start_time,file_name,file_size,duration,status,trigger,engine)
    error=None
    load.insert_validate_result(run_id,total_rows,missing_values,duplicate_rows_dropped,error,engine)
    
    visualise.map_graph_types(run_id,analysis_result)
    
    insertion_date=datetime.today()
    file_size=round((file_size/1024/1024),2)
    file_data={"file_name":file_name,"file_size":file_size,"file_inserted":insertion_date}
    file_info={"total_rows":total_rows,"total_columns":cleaned_data.shape[1],"missing_values":missing_values,"duplicate_rows_dropped":duplicate_rows_dropped}
    output_data={"file_data":file_data,"file_info":file_info,"analysis_result":analysis_result,"discarded_queries":discarded_queries}
    file_path=Path(f"outputs/json/results_{run_id}.json")
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path,"w",encoding='utf-8') as f:
        json.dump(output_data,f,default=str,indent=4)
    
    logger.info("Pipeline runs Successfully")
    
except Exception as e:
    status="Failed"
    file_name=None
    file_size=None
    duration = int((datetime.now() - start_time).total_seconds())
    if engine:
        run_id = load.insert_pipeline_runs(start_time,file_name,file_size,duration,status,trigger,engine)
    logger.error(f"Pipeline failed to run : {e}")
        
    