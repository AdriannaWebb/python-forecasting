# main.py
import pandas as pd
import sys
from datetime import datetime
from scripts.extract import extract_business
from scripts.transform import (
    clean_business_data, 
    create_monthly_business_summary,
    prepare_summary_for_forecast,
    correct_known_data_issues
)
from scripts.forecast import generate_business_forecast_from_summary
from scripts.load import save_forecast_data, save_monthly_summary
from scripts.logging_setup import logger

def main():
    """Main function to execute the forecasting process"""
    try:
        # Add console output for better visibility
        print("Starting forecast process...")
        sys.stdout.flush()  # Force output to be displayed immediately
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        logger.info(f"Starting forecast process at {timestamp}")
        
        # Extract
        print("Extracting data...")
        sys.stdout.flush()
        logger.info("Extracting data...")
        raw_data = extract_business()
        logger.info(f"Data extracted successfully. Shape: {raw_data.shape}")
        
        # Transform - Monthly Summary (back to 2000)
        print("Creating monthly business summary...")
        sys.stdout.flush()
        logger.info("Creating monthly business summary...")
        clean_data = clean_business_data(raw_data)
        monthly_summary = create_monthly_business_summary(clean_data, start_year=2000)
        logger.info(f"Monthly summary created. Shape: {monthly_summary.shape}")
        
        # Apply corrections to known data issues
        print("Applying corrections to known data issues...")
        sys.stdout.flush()
        logger.info("Applying corrections to known data issues...")
        corrected_summary = correct_known_data_issues(monthly_summary)
        logger.info("Corrections applied successfully")
        
        # Save Monthly Summary
        print("Saving monthly summary...")
        sys.stdout.flush()
        logger.info("Saving monthly summary...")
        summary_file = save_monthly_summary(corrected_summary)
        logger.info(f"Monthly summary saved to {summary_file}")
        
        # Forecast using monthly summary data
        print("Generating forecast from monthly summary...")
        sys.stdout.flush()
        logger.info("Generating forecast from monthly summary...")
        forecast_periods = 12  # Forecast for 1 year
        lookback_years = 5     # Use 5 years of data
        forecast_methods = ['simple', 'ets']  # Use both simple and ETS methods
        forecast_data = generate_business_forecast_from_summary(
            corrected_summary, 
            forecast_periods=forecast_periods,
            lookback_years=lookback_years,
            methods=forecast_methods
        )
        logger.info(f"Forecast generated successfully. Shape: {forecast_data.shape}")
        
        # Save Forecast
        print("Saving forecast data...")
        sys.stdout.flush()
        logger.info("Saving forecast data...")
        forecast_file = save_forecast_data(forecast_data)
        logger.info(f"Forecast saved to {forecast_file}")
        
        print("Forecast process completed successfully!")
        sys.stdout.flush()
        logger.info("Forecast process completed successfully")
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        import traceback
        print(traceback.format_exc())
        logger.error(f"Error in forecast process: {e}")
        logger.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    main()