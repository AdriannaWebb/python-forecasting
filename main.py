# main.py
import pandas as pd
from datetime import datetime
from scripts.extract import extract_business
from scripts.transform import (
    clean_business_data, 
    filter_data_by_date_range,
    aggregate_monthly_data, 
    create_time_series_df,
    add_date_features,
    create_monthly_business_summary
)
from scripts.forecast import generate_business_forecast
from scripts.load import save_forecast_data, save_monthly_summary
from scripts.logging_setup import logger

def main():
    """Main function to execute the forecasting process"""
    try:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info(f"Starting forecast process at {timestamp}")
        
        # Extract
        logger.info("Extracting data...")
        raw_data = extract_business()
        
        # Transform - Monthly Summary (back to 2000)
        logger.info("Creating monthly business summary from 2000...")
        clean_data = clean_business_data(raw_data)
        monthly_summary = create_monthly_business_summary(clean_data, start_year=2000)
        
        # Save Monthly Summary
        logger.info("Saving monthly business summary...")
        summary_file = save_monthly_summary(monthly_summary)
        logger.info(f"Monthly summary saved to {summary_file}")
        
        # Transform - for forecasting (using 7 years of data)
        logger.info("Preparing data for forecasting...")
        filtered_data = filter_data_by_date_range(clean_data, lookback_years=7)
        monthly_joins, monthly_drops = aggregate_monthly_data(filtered_data)
        time_series_data = create_time_series_df(monthly_joins, monthly_drops)
        time_series_data = add_date_features(time_series_data)
        
        # Forecast
        logger.info("Generating forecast...")
        forecast_periods = 12  # Forecast for 1 year
        forecast_method = 'ensemble'  # Use ensemble of methods
        forecast_data = generate_business_forecast(
            time_series_data, 
            forecast_periods=forecast_periods,
            method=forecast_method
        )
        
        # Save Forecast
        logger.info("Saving forecast data...")
        forecast_file = save_forecast_data(forecast_data)
        logger.info(f"Forecast saved to {forecast_file}")
        
        logger.info("Forecast process completed successfully")
    except Exception as e:
        logger.error(f"Error in forecast process: {e}")
        raise

if __name__ == "__main__":
    main()