# scripts/load.py
import pandas as pd
import os
from datetime import datetime
from scripts.logging_setup import logger
from config import OUTPUT_DIR

def save_to_excel(df, filename, sheet_name='Forecast'):
    """
    Save a dataframe to an Excel file
    """
    try:
        file_path = os.path.join(OUTPUT_DIR, filename)
        logger.info(f"Saving data to: {file_path}")
        
        with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            
        logger.info(f"Successfully saved {len(df)} rows to {filename}")
        return file_path
    except Exception as e:
        logger.error(f"Error saving to Excel: {e}")
        raise

def save_forecast_data(forecast_df, timestamp=None):
    """
    Save forecast data to Excel with fixed filename
    
    Parameters:
    - forecast_df: DataFrame containing forecast data
    - timestamp: Optional timestamp (not used, kept for compatibility)
    
    Returns:
    - File path of saved Excel file
    """
    try:
        # Use a fixed filename without timestamp
        filename = "business_forecast.xlsx"
        
        # Format the DataFrame for Excel
        excel_df = forecast_df.copy()
        
        # Add formatted date column
        excel_df['date'] = excel_df['year_month'].dt.strftime('%b %Y')
        
        # Ensure is_forecast column exists
        if 'is_forecast' not in excel_df.columns:
            excel_df['is_forecast'] = False
            
        # Reorder columns for better readability
        column_order = [
            'date', 'year', 'month', 'month_name', 'is_forecast',
            'active_businesses', 'new_joins', 'new_drops', 'net_change'
        ]
        
        # Add any additional columns that weren't specified in the order
        for col in excel_df.columns:
            if col not in column_order and col != 'year_month':
                column_order.append(col)
                
        # Filter to include only columns that exist in the DataFrame
        available_columns = [col for col in column_order if col in excel_df.columns]
        excel_df = excel_df[available_columns]
        
        # Save to Excel
        file_path = save_to_excel(excel_df, filename, 'Forecast')
        
        return file_path
    except Exception as e:
        logger.error(f"Error saving forecast data: {e}")
        raise

def save_monthly_summary(summary_df, timestamp=None):
    """
    Save monthly business summary to Excel with fixed filename
    """
    try:
        # Use a fixed filename without timestamp
        filename = "business_monthly_summary.xlsx"
        
        # Format the DataFrame for Excel
        excel_df = summary_df.copy()
        
        # Add formatted date column
        excel_df['date'] = excel_df['year_month'].dt.strftime('%b %Y')
        
        # Reorder columns for better readability
        column_order = [
            'date', 'year', 'month', 'month_name',
            'active_businesses', 'new_joins', 'new_drops'
        ]
        
        # Add any additional columns that weren't specified in the order
        for col in excel_df.columns:
            if col not in column_order and col != 'year_month':
                column_order.append(col)
                
        excel_df = excel_df[column_order]
        
        # Save to Excel
        file_path = save_to_excel(excel_df, filename, 'Monthly Summary')
        
        return file_path
    except Exception as e:
        logger.error(f"Error saving monthly summary: {e}")
        raise