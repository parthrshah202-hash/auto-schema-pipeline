from dotenv import load_dotenv
from sqlalchemy import create_engine,text
import logging
import os

load_dotenv()

host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")

logger = logging.getLogger(__name__)

def get_connection():
    try:
        db_url = f"postgresql+psycopg2://{user}:{password}@{host}/{db_name}"
        engine=create_engine(db_url)
        with engine.connect() as connection:
            logger.info("Connected to Database successfully")
            return engine
    except Exception as e:
        logger.error(f"Database setup failed: {e}")
        exit(1)
        
def set_up_tables(engine):
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
            logger.error("Failed to setup pipeline_runs Table")
            exit(1)
        
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
            logger.error("Failed to setup validate_result Table")
            exit(1)
    
    
def insert_pipeline_runs(stamp,name,size,duration,status,trigger,engine):
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
            
            logger.info("Row added in pipeline_runs successfully")
            return run_id
        except Exception as e:
            logger.error(f"Row failed to be added in pipeline_runs : {e}")
            exit(1)
        
def insert_validate_result(run_id,total_rows,duplicates_dropped,values_replaced,error_message,engine):
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
            exit(1)
        
def create_table(table_name,schema_dict,engine):
    with engine.connect() as connection:
        try:
            columns=[f"{col_name} {col_type}" for col_name,col_type in schema_dict.items()]
            query=text(f"CREATE TABLE IF NOT EXISTS {table_name} (\n    " + ",\n    ".join(columns) + "\n);")
            connection.execute(query)
            connection.commit()
            logger.info(f"{table_name} created successfully")
        except Exception as e:
            logger.error(f"{table_name} creation failed : {e}")
            
def insert_data(table_name,df,engine):
    with engine.connect() as connection:
        try:
            df.to_sql(name=table_name,con=engine,if_exists='append')
            logger.info(f"Data added in {table_name} successfully")
        except Exception as e:
            logger.error(f"Addition of data to {table_name} failed : {e}")