from dotenv import load_dotenv
from sqlalchemy import create_engine,text,inspect
from datetime import datetime
from urllib.parse import quote_plus
import time
import logging
import os

load_dotenv()

logger = logging.getLogger(__name__)

def get_connection():
    """Establish a connection factory for the PostgreSQL database.
    
    Try to establish a connection factory by reading URL componenets from .env file.Try to connect 3 times and then if failed, log and raise the error

    Returns:
        sqlalchemy.engine.Engine: The engine instance used to interact with the database.

    Raises:
        Exception: If the database URL is malformed or the connection 
            cannot be established.
    """
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    
    max_retries=3
    for attempt in range (max_retries):
        try:
            db_url = f"postgresql+psycopg2://{user}:{quote_plus(password)}@{host}:{port}/{db_name}"
            engine=create_engine(db_url)
            with engine.connect() as connection:
                logger.info("Connected to Database successfully")
                return engine
        except Exception as e:
            logger.error(f"Attempt {attempt+1} to setup Database failed: {e}")
            if attempt < max_retries-1:
                time.sleep(5)
            else:
                logger.error("All attempts to setup Database failed")
                raise
        
def set_up_tables(engine):
    """Initialize the database schema for pipeline tracking.
    
    Creates the 'pipeline_runs' and 'validate_result' tables if they do 
    not already exist.
    
    Args:
        engine (Engine): The SQLAlchemy engine instance used to establish a database connection.
            
    Raises:
        Exception: If the SQL execution fails due to permissions, 
            connectivity issues, or syntax errors.
    """
    with engine.connect() as connection:
        try:
            connection.execute(text("""
                        CREATE TABLE IF NOT EXISTS pipeline_runs(
                            run_id SERIAL PRIMARY KEY,
                            runtime_stamp TIMESTAMP,
                            file_name TEXT,
                            file_size BIGINT,
                            duration INT,
                            status VARCHAR(20),
                            triggered_by TEXT
                        )
                        """))
            logger.info("pipeline_runs Table created successfully")
            connection.commit()
        except Exception as e:
            logger.error(f"Failed to setup pipeline_runs Table : {e}")
            raise
        
        try:
            connection.execute(text("""
                        CREATE TABLE IF NOT EXISTS validate_result(
                            id SERIAL PRIMARY KEY,
                            run_id INT REFERENCES pipeline_runs(run_id),
                            total_rows BIGINT,
                            duplicates_dropped INT,
                            values_replaced INT,
                            error_message VARCHAR(50)
                        )
                        """))
            logger.info("validate_result Table created successfully")
            connection.commit()
        except Exception as e:
            logger.error(f"Failed to setup validate_result Table : {e}")
            raise
    
    
def insert_pipeline_runs(stamp,name,size,duration,status,trigger,engine):
    """Insert a new entry in pipeline_runs table.
    
    Args:
        stamp (datetime): The time when pipeline started
        name (str): Name of the file uploaded
        size (int): Size of the file uploaded
        duration (int): Total duration of pipeline
        status (str): If the execution was succesfull or not
        trigger (str): If pipeline was triggered manually or automatically
        engine (Engine): The SQLAlchemy engine instance used to establish a database connection.
        
    Returns:
        run_id (int): The unique id generated for this pipeline run
            
    Raises:
        Exception: If the SQL execution fails due to permissions, 
            connectivity issues, or syntax errors.
    """
    with engine.connect() as connection:
        try:
            query = text("""
                INSERT INTO pipeline_runs (runtime_stamp, file_name, file_size, duration, status, triggered_by)
                VALUES (:stamp, :name, :size, :duration, :status, :trigger)
                RETURNING run_id
            """)
            
            params = {
            "stamp": stamp,
            "name": name,
            "size": size,
            "duration": duration,
            "status": status,
            "trigger": trigger
            }
            result=connection.execute(query, params)
            run_id=result.scalar()
            connection.commit()
            
            logger.info(f"Row added in pipeline_runs successfully with run_id : {run_id}")
            return run_id
        except Exception as e:
            logger.error(f"Row failed to be added in pipeline_runs : {e}")
            raise
        
def insert_validate_result(run_id,total_rows,duplicates_dropped,values_replaced,error_message,engine):
    """Insert a new entry in validate_result table.
    
    Args:
        run_id (int): The unique id generated for this pipeline run
        total_rows (int): Total number of rows in dataset
        duplicates_dropped (int): Total number of duplicate values(dropped) in dataset
        values_replaced (int): Total number of values replaced in dataset
        error_message (str): A description of any issues encountered, or 'None' if successful.
        engine (Engine): The SQLAlchemy engine instance used to establish a database connection. 
            
    Raises:
        Exception: If the database insertion fails due to constraint violations (like a non-existent run_id) or connectivity issues.
    """
    with engine.connect() as connection:
        try:
            query = text("""
                INSERT INTO validate_result (run_id, total_rows, duplicates_dropped, values_replaced, error_message)
                VALUES (:run_id, :total_rows, :duplicates_dropped, :values_replaced, :error_message)
            """)
            
            params = {
            "run_id":run_id, 
            "total_rows":total_rows, 
            "duplicates_dropped":duplicates_dropped, 
            "values_replaced":values_replaced, 
            "error_message":error_message
            }
            connection.execute(query, params)
            connection.commit()
            
            logger.info("Row added in validate_result successfully")
        except Exception as e:
            logger.error(f"Row failed to be added in validate_result : {e}")
            raise
        
def check_table_existance(table_name,engine):
    """Check if table with a name exists in database and modifies it.
    
    To avoid data being appeneded in same table, we check if it already exists in database. If yes, we modify the table_name. Also, we truncate the tabel name just in case it exceeds Postgre SQL table name limit (63)

    Args:
        table_name (str): Name with which we have to check if a table exists in database
        engine (Engine): he SQLAlchemy engine instance used to establish a database connection.
        
    Returns:
        str: Name with which table is to be created in database
    """
    inspector=inspect(engine)
    
    if(len(table_name)>47):
        table_name=table_name[:45]
    
    if inspector.has_table(table_name):
        table_name=table_name+str(datetime.now().strftime('%Y%m%d_%H%M%S'))
        
    return table_name
        
def create_table(table_name,schema_dict,engine):
    """Create new database table according to dynamic schema

    Args:
        table_name (str): Name with which table is to be created in database
        schema_dict (dict): A dictionary mapping column names (keys) to SQL data types (values)
        engine (Engine): The SQLAlchemy engine instance used to establish a database connection. 
        
    Raises:
        Exception: If the SQL construction is malformed or the database connection fails during execution.

    """
    with engine.connect() as connection:
        try:
            columns=[f"{col_name} {col_type}" for col_name,col_type in schema_dict.items()]
            query=text(f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns) + "\n);")
            connection.execute(query)
            connection.commit()
            logger.info(f"{table_name} created successfully")
        except Exception as e:
            logger.error(f"{table_name} creation failed : {e}")
            raise
            
def insert_data(table_name,df,engine):
    """Insertion of data in database table with name as table_name

    Args:
        table_name (str): Name with which table is to be created in database
        df (DataFrame): The dataset containing the data to be inserted in database table with name as table_name
        engine (Engine): The SQLAlchemy engine instance used to establish a database connection. 
        
    Raises:
        Exception: If the insertion fails due to schema mismatches, connectivity issues, or database constraint violations.
    """
    with engine.connect() as connection:
        try:
            df.to_sql(name=table_name,con=engine,if_exists='append',index=False)
            logger.info(f"Data added in {table_name} successfully")
        except Exception as e:
            logger.error(f"Addition of data to {table_name} failed : {e}")
            raise
        

def get_run_history(engine):
    with engine.connect() as connection:
        try:
            query=text("""SELECT run_id,runtime_stamp,file_name,file_size,duration,status
                       FROM pipeline_runs
                       ORDER BY run_id DESC
                       LIMIT 10
                       """)
            result=connection.execute(query)
            pipeline_history=[dict(row) for row in result.mappings()]
            logger.info("Successfully obtained the last 10 rows of pipeline_runs table")
            return pipeline_history
        except Exception as e:
            logger.error(f"Extraction of last 10 rows of pipeline_runs table failed : {e}")
            raise
            
    