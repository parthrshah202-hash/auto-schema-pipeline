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
import report
import itertools
import json


logging.basicConfig(
    filename="logs/pipeline.log",
    format='%(asctime)s %(levelname)s: %(message)s',
    filemode='a'
)

logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

def run_pipeline(path,trigger="Manual"):
    """Orchestrate the end-to-end data processing and analysis lifecycle.
    
    This function coordinates the ingestion, transformation, schema detection, 
    AI analysis, and reporting modules and serves as the single entry point 
    for the Auto Schema Pipeline.

    Args:
        path (Path): Filesystem path to the raw input dataset
        trigger (str, optional):The source of the execution trigger. Defaults to "Manual".

    Returns:
        tuple: A result pair containing:
            - run_id (int | None): The database primary key for this execution.
            - error_message (str | None): A descriptive error string if the 
              process failed; otherwise None.
    """
    
    start_time=datetime.now()
    engine=None
    run_id=None
    file_name=None
    file_size=None
    try:
        #Establish connection
        engine=load.get_connection()
        
        #Set up database tables
        load.set_up_tables(engine)
        
        #Get raw data
        raw_data,file_name,file_size = ingestion.load_data(path)
        if raw_data.empty:
            raise ValueError("Raw DataFrame is empty, cannot transform dataset.")

        #Get cleaned data
        cleaned_data,total_rows,missing_values,duplicate_rows_dropped = transform.clean_data(raw_data)
        
        if cleaned_data.empty:
            raise ValueError("Cleaned DataFrame is empty, cannot detect schema.")
        
        #Detect schema for the input dataset
        schema_dict=schema_detector.detect_schema(cleaned_data)
        
        table_name=file_name
        
        #Check existance of table in database with same name
        table_name=load.check_table_existance(table_name,engine)
        
        #Create table with correct name and insert data for the data inserted by user
        load.create_table(table_name,schema_dict,engine)
        load.insert_data(table_name,cleaned_data,engine)
        
        #Prompt the AI and get analysis
        prompt=gemini.build_prompt(cleaned_data,file_name,table_name,schema_dict,total_rows,duplicate_rows_dropped)
        analysis_dict=gemini.get_analysis(prompt)
        
        #Validate and execute SQL queries
        valid_queries,discarded_queries=validate_query.validate(analysis_dict,table_name,schema_dict)
        analysis_result=execute_query.execute_query(valid_queries,engine)
        if not analysis_result:
            logger.warning("No valid queries were executed. Analysis result is empty.")
        
        for question,answer in list(analysis_result.items()):
            if len(answer) > 10:
                if(isinstance(answer,list)):
                    analysis_result[question]=answer[:10]
                elif(isinstance(answer,dict)):
                    analysis_result[question]=dict(itertools.islice(answer.items(), 10))
                    
        
        end_time=datetime.now()
        duration=int((end_time-start_time).total_seconds())
        status="Success"
        
        
        run_id = load.insert_pipeline_runs(start_time,file_name,file_size,duration,status,trigger,engine)
        
        #Create and generate JSON file
        file_size=round((file_size/1024/1024),2)
        insertion_date=datetime.today()
        file_data={"file_name":file_name,"file_size":file_size,"file_inserted":insertion_date}
        file_info={"total_rows":total_rows,"total_columns":cleaned_data.shape[1],"missing_values":missing_values,"duplicate_rows_dropped":duplicate_rows_dropped}
        output_data={"file_data":file_data,"file_info":file_info,"analysis_result":analysis_result,"discarded_queries":discarded_queries}
        file_path=Path(f"outputs/json/results_{run_id}.json")
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path,"w",encoding='utf-8') as f:
            json.dump(output_data,f,default=str,indent=4)
        
        #Insert into validate result table
        try:
            error=None
            load.insert_validate_result(run_id,total_rows,duplicate_rows_dropped,missing_values,error,engine)
        except Exception as e:
            logger.warning(f"Failed to insert validated result in Database : {e}")
        
        #Generate plots
        try:
            visualise.map_graph_types(run_id,analysis_result)
        except Exception as e:
            logger.warning(f"Failed to generate charts : {e}")
            
        #Generate pdf report
        try:
            report.create_report(run_id)
        except Exception as e:
            logger.warning(f"Failed to generate PDF report : {e}")
        
        logger.info("Pipeline runs Successfully")
        return run_id , None
        
    except Exception as e:
        status="Failed"
        duration = int((datetime.now() - start_time).total_seconds())
        if engine:
            run_id = load.insert_pipeline_runs(start_time,file_name,file_size,duration,status,trigger,engine)

        logger.error(f"Pipeline failed to run : {e}")
        return None, str(e)
        
#gurad to run piepline from terminal
if __name__=="__main__":
    run_pipeline("data/raw/server_logs.csv")