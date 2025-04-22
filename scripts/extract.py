# scripts/extract.py
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from scripts.logging_setup import logger
from config import DB_CONFIG

# Modified extract.py function
def create_connection():
    """Create a connection to the MySQL database"""
    try:
        # Use the host as-is without appending the port
        connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        engine = create_engine(connection_string)
        logger.info("Database connection created successfully")
        return engine
    except Exception as e:
        logger.error(f"Error creating database connection: {e}")
        raise

def extract_business():
    """Extract data from the Business table - selecting only specific columns"""
    try:
        engine = create_connection()
        # Modified query to only select the columns you need
        query = "SELECT bid, date_accredited, date_dropped FROM Business"
        logger.info("Extracting specific columns from Business table")
        df = pd.read_sql(query, engine)
        logger.info(f"Business data extracted successfully. Shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error extracting Business data: {e}")
        raise

def extract_custom_query(query):
    """Extract data using a custom query if needed"""
    try:
        engine = create_connection()
        logger.info(f"Executing custom query: {query}")
        df = pd.read_sql(query, engine)
        logger.info(f"Data extracted successfully. Shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error executing custom query: {e}")
        raise