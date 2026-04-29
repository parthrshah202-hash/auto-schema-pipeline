from sqlalchemy import text
import logging

logger = logging.getLogger(__name__)

def execute_query(valid_queries,engine):
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