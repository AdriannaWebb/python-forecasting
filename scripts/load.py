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
    Save forecast data to Excel with timestamp in filename
    """
    try:
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
        filename = f"business_forecast_{timestamp}.xlsx"
        
        # Format the DataFrame for Excel
        excel_df = forecast_df.copy()
        
        # Add formatted date column
        excel_df['date'] = excel_df['year_month'].dt.strftime('%b %Y')
        
        # Reorder columns for better readability
        column_order = [
            'date', 'year', 'month', 'month_name', 'is_forecast',
            'join_count', 'drop_count', 'net_change', 'cumulative_total'
        ]
        
        # Add any additional columns that weren't specified in the order
        for col in excel_df.columns:
            if col not in column_order:
                column_order.append(col)
                
        excel_df = excel_df[column_order]
        
        # Save to Excel
        file_path = save_to_excel(excel_df, filename, 'Forecast')
        
        return file_path
    except Exception as e:
        logger.error(f"Error saving forecast data: {e}")
        raise

def save_monthly_summary(summary_df, timestamp=None):
    """
    Save monthly business summary to Excel
    """
    try:
        if timestamp is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
        filename = f"business_monthly_summary_{timestamp}.xlsx"
        
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