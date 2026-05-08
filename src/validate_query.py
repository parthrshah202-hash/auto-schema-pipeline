import logging

logger = logging.getLogger(__name__)

def validate(analysis_dict,table_name,schema_dict):
    """Check the correctness of SQL queries returned by AI

    Args:
        analysis_dict (dict): A dictionary where keys are Questions and values are the equivalent SQL Queries.
        table_name (str): The identifier of the table in the database.
        schema_dict (dict): A dictionary mapping column names (keys) to SQL data types (values)

    Returns:
        tuple: A collection of processing results:
            - dict: A dictionary mapping question (keys) to the valid SQL query (value)
            - dict: A dictionary mapping question (keys) to the discarded query and the reason for exculsion (value)
    """
    valid_queries={}
    discarded_queries={}
    
    for question,query in analysis_dict.items():
        error_reason=""
        
        if not query.upper().startswith("SELECT"):
            error_reason="Query does not start with select"
            
        elif table_name not in query:
            error_reason="Query does not contain correct table name"
            
        elif not any(column_name in query for column_name in schema_dict):
            error_reason="Query does not conatin correct column"
            
        if error_reason:
            discarded_queries[question] = {
                "query": query, 
                "reason": error_reason
            }
            logger.warning(f"Discarded '{question}': {error_reason}")
        else:
            valid_queries[question] = query
            
    logger.info(f"Validated {len(analysis_dict)} queries.")
    return valid_queries, discarded_queries