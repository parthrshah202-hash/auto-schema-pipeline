import logging

logger = logging.getLogger(__name__)

def validate(analysis_dict,table_name,schema_dict):
    valid_queries={}
    discarded_queries={}
    
    for question,query in analysis_dict.items():
        is_query_valid=True
        error_reason=""
        
        if not query.upper().startswith("SELECT"):
            is_query_valid=False
            error_reason="Query does not start with select"
            
        elif table_name not in query:
            is_query_valid=False
            error_reason="Query does not contain correct table name"
            
        elif not any(column_name in query for column_name in schema_dict):
            is_query_valid = False
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