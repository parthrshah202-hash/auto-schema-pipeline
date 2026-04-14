import psycopg2
import logging

logger = logging.getLogger(__name__)

try:
    conn=psycopg2.connect(
        host="localhost",
        database="pipeline_db",
        user="postgres",
        password="***REMOVED***"
        )
    logger.info("Connected to Database successfully")

    cursor=conn.cursor()
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
    logger.error(f"Database setup failed: {e}")
    exit(1)
    
    
def insert_pipeline_runs(stamp,name,size,duration,status,trigger):
    try:
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
        
def insert_validate_result(run_id,total_rows,duplicates_dropped,values_replaced,error_message):
    try:
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