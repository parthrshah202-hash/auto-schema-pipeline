from dotenv import load_dotenv
from google import genai
import logging
import json
import os

from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

logger = logging.getLogger(__name__)

def build_prompt(df,file_name,table_name,schema_dict,total_rows,duplicate_rows_dropped):
    """Create the unique prompt according to the input dataset
    
    Generates a structured prompt containing metadata, schema information, and a data preview to guide Gemini in generating SQL-based insights.
    
    Args:
        df(DataFrame): The input dataset
        file_name(str): Name of source file
        table_name(str): The identifier of the table in the database.
        schema_dict (dict): A dictionary mapping column names (keys) to SQL data types (values)
        total_rows(int): Total number of rows in dataset
        duplicates_rows_dropped (int): Total number of duplicate values(dropped) in dataset
        
    Returns:
        str: A formatted prompt string configured for a JSON-only AI response.
    """
    
    prompt=f"""Analyse this from the cleaned dataset named {file_name} and tablename is {table_name}.It has {total_rows} total rows.
    {duplicate_rows_dropped} total rows were dropped as they were duplicate.
    These are the first 5 rows of the dataset {df.head(5)}
    This is a dictionary mapping column headings and their types {schema_dict}. 
    Suggest some useful analysis(questions which can give important insights about the dataset) 
    and also give correct SQL queries to get answers to those questions..Respond in JSON only,
    no extra text, no explanation nothing extra .Just follow this exact JSON structure-
    {{ "question 1": "SQL query 1", "question 2": "SQL query 2" }}"""
    logger.info("Prompt built successfully")
    return prompt
    
        
def get_analysis(prompt):
    """Request and parse an AI-driven SQL analysis from Gemini.
    
    Args:
        prompt(str): A formatted prompt string configured for a JSON-only AI response.
        
    Returns:
        dict: A dictionary where keys are Questions and values are the equivalent SQL Queries.
        
    Raises:
        Exception: If the API call fails, or if the AI response cannot be parsed into a valid dictionary.
    """
    try:
        gemini_api_key=os.getenv("GEMINI_API_KEY")
        client = genai.Client(api_key=gemini_api_key)
        response=client.models.generate_content(
            model="gemini-flash-latest", 
            contents=prompt
        )
        logger.info("AI analysis suggestions generated successfully")
        analysis_dict=json.loads(response.text)
        return analysis_dict
    except Exception as e:
        logger.error(f"Failed to generate AI analysis suggestions : {e}")
        raise
        


 