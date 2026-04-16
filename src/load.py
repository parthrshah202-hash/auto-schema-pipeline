import psycopg2
import logging
from dotenv import load_dotenv
import os

load_dotenv()

host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

logger = logging.getLogger(__name__)

def get_connection():
    try:
        conn=psycopg2.connect(
            host=host,
            database="db_name",
            user="user",
            password="password"
            )
        logger.info("Connected to Database successfully")
        return conn
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        exit(1)
        
def set_up_tables(conn):
    cursor=conn.cursor()
    try:
        cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs(
                        run_id SERIAL PRIMARY KEY,
                        runtime_stamp TIMESTAMP,
                        file_name TEXT,
                        file_size BIGINT,
                        duration INT,
                        status VARCHAR(20),
                        triggered_by TEXT
                    )
                    """)
        logger.info("pipeline_runs Table created successfully")
        conn.commit()
    except Exception as e:
        logger.error("Failed to setup pipeline_runs Table")
        exit(1)
        
    try:
        cursor.execute("""
                    CREATE TABLE IF NOT EXISTS validate_result(
                        id SERIAL PRIMARY KEY,
                        run_id INT REFERENCES pipeline_runs(run_id),
                        total_rows BIGINT,
                        duplicates_dropped INT,
                        values_replaced INT,
                        error_message VARCHAR(50)
                    )
                    """)
        logger.info("validate_result Table created successfully")
        conn.commit()
    except Exception as e:
        logger.error("Failed to setup validate_result Table")
        exit(1)
    
    
def insert_pipeline_runs(stamp,name,size,duration,status,trigger,conn):
    try:
        cursor=conn.cursor()
        query = """
            INSERT INTO pipeline_runs (runtime_stamp, file_name, file_size, duration, status, triggered_by)
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING run_id
        """
        params = (stamp, name, size, duration, status, trigger)
        cursor.execute(query, params)
        run_id=cursor.fetchone()[0]
        conn.commit()
        
        logger.info("Row added in pipeline_runs successfully")
        return run_id
    except Exception as e:
        logger.error(f"Row failed to be added in pipeline_runs : {e}")
        exit(1)
        
def insert_validate_result(run_id,total_rows,duplicates_dropped,values_replaced,error_message,conn):
    try:
        cursor=conn.cursor()
        query = """
            INSERT INTO validate_result (run_id, total_rows, duplicates_dropped, values_replaced, error_message)
            VALUES (%s, %s, %s, %s, %s)
        """
        params = (run_id, total_rows, duplicates_dropped, values_replaced, error_message)
        cursor.execute(query, params)
        conn.commit()
        
        logger.info("Row added in validate_result successfully")
    except Exception as e:
        logger.error(f"Row failed to be added in validate_result : {e}")
        exit(1)
        
def create_table(table_name,schema_dict,conn):
    try:
        cursor=conn.cursor()
        columns=[f"{col_name} {col_type}" for col_name,col_type in schema_dict.items()]
        query=f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns) + "\n);"
        cursor.execute(query)
        conn.commit()
        logger.info(f"{table_name} created successfully")
    except Exception as e:
        logger.error(f"{table_name} creation failed : {e}")