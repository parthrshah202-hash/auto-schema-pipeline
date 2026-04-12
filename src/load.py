import psycopg2
import logging

logging.basicConfig(
    filename="logs/load.log",
    format='%(asctime)s %(levelname)s : %(message)s',
    filemode='a'
)

logger=logging.getLogger()
logger.setLevel(logging.DEBUG)

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
                    rows_deleted INT,
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