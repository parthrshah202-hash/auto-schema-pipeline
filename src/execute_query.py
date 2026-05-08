from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def execute_query(valid_queries,engine):
    """Execute validated SQL queries and retrieve results as structured data
    
    Iterates through a collection of SQL queries, executes them against the connected database, and parses the result sets into a JSON-serializable format.

    Args:
        valid_queries (dict): A dictionary mapping question (keys) to the valid SQL query (value)
        engine (Engine): The SQLAlchemy engine instance used for database execution.
    
    Returns:
        dict: A dictionary where keys are the original questions and values are lists of dictionaries, each representing a row in the result set.
    """
    analysis={}
    
    with engine.connect() as connection:
        for question,query in valid_queries.items():
            try:
                result=connection.execute(text(query))
                answer = [dict(row._mapping) for row in result.fetchall()]
                logger.info(f"Query executed successfully for question : '{question}'")
                analysis[question]=answer
            except Exception as e:
                logger.error(f"Query for {question} could not be executed : {e}")
        return analysis