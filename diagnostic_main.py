# diagnostic_main.py
import pandas as pd
import os
import sys
from datetime import datetime
import traceback

# Setup basic console logging first (in case the logger import fails)
def print_checkpoint(message):
    print(f"CHECKPOINT: {message}")
    sys.stdout.flush()  # Force output to be displayed immediately

print_checkpoint("Starting diagnostic script")

# Try importing the logger
try:
    from scripts.logging_setup import logger
    print_checkpoint("Logger imported successfully")
except Exception as e:
    print(f"ERROR IMPORTING LOGGER: {str(e)}")
    print(traceback.format_exc())
    # Create a basic logger as fallback
    import logging
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("diagnostic")
    logger.setLevel(logging.INFO)

# Check if output directory exists and is writable
try:
    print_checkpoint("Checking output directory")
    from config import OUTPUT_DIR
    if not os.path.exists(OUTPUT_DIR):
        print(f"Output directory does not exist: {OUTPUT_DIR}")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        print(f"Created output directory: {OUTPUT_DIR}")
    
    # Test if we can write to the output directory
    test_file = os.path.join(OUTPUT_DIR, "test_write.txt")
    with open(test_file, 'w') as f:
        f.write("Test write access")
    os.remove(test_file)
    print_checkpoint("Output directory is writable")
except Exception as e:
    print(f"ERROR WITH OUTPUT DIRECTORY: {str(e)}")
    print(traceback.format_exc())

# Try importing each module separately
try:
    print_checkpoint("Importing extract module")
    from scripts.extract import extract_business
    print_checkpoint("Extract module imported successfully")
except Exception as e:
    print(f"ERROR IMPORTING EXTRACT MODULE: {str(e)}")
    print(traceback.format_exc())

try:
    print_checkpoint("Importing transform module")
    from scripts.transform import (
        clean_business_data, 
        create_monthly_business_summary,
        prepare_summary_for_forecast,
        correct_known_data_issues
    )
    print_checkpoint("Transform module imported successfully")
except Exception as e:
    print(f"ERROR IMPORTING TRANSFORM MODULE: {str(e)}")
    print(traceback.format_exc())

try:
    print_checkpoint("Importing forecast module")
    from scripts.forecast import generate_business_forecast_from_summary
    print_checkpoint("Forecast module imported successfully")
except Exception as e:
    print(f"ERROR IMPORTING FORECAST MODULE: {str(e)}")
    print(traceback.format_exc())

try:
    print_checkpoint("Importing load module")
    from scripts.load import save_forecast_data, save_monthly_summary
    print_checkpoint("Load module imported successfully")
except Exception as e:
    print(f"ERROR IMPORTING LOAD MODULE: {str(e)}")
    print(traceback.format_exc())

# Run each step individually
def diagnostic_run():
    """Run each step of the main process separately with detailed logging"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    print_checkpoint(f"Starting diagnostic forecast process at {timestamp}")
    
    # Step 1: Extract
    try:
        print_checkpoint("STEP 1: Extracting data")
        raw_data = extract_business()
        print_checkpoint(f"Extraction successful. Shape: {raw_data.shape}")
        print(f"First 5 rows of raw data:\n{raw_data.head()}")
        print(f"Data types: {raw_data.dtypes}")
        
        if raw_data.empty:
            print("WARNING: Raw data is empty!")
        elif len(raw_data) < 10:
            print("WARNING: Very few rows in raw data!")
    except Exception as e:
        print(f"ERROR IN EXTRACTION: {str(e)}")
        print(traceback.format_exc())
        return
    
    # Step 2: Clean data
    try:
        print_checkpoint("STEP 2: Cleaning data")
        clean_data = clean_business_data(raw_data)
        print_checkpoint(f"Data cleaning successful. Shape: {clean_data.shape}")
        print(f"First 5 rows of clean data:\n{clean_data.head()}")
        
        if clean_data.empty:
            print("WARNING: Clean data is empty!")
        elif len(clean_data) < 10:
            print("WARNING: Very few rows in clean data!")
    except Exception as e:
        print(f"ERROR IN DATA CLEANING: {str(e)}")
        print(traceback.format_exc())
        return
    
    # Step 3: Create monthly summary
    try:
        print_checkpoint("STEP 3: Creating monthly business summary")
        monthly_summary = create_monthly_business_summary(clean_data, start_year=2000)
        print_checkpoint(f"Monthly summary created successfully. Shape: {monthly_summary.shape}")
        print(f"First 5 rows of monthly summary:\n{monthly_summary.head()}")
        print(f"Last 5 rows of monthly summary:\n{monthly_summary.tail()}")
        
        if monthly_summary.empty:
            print("WARNING: Monthly summary is empty!")
        elif len(monthly_summary) < 10:
            print("WARNING: Very few rows in monthly summary!")
    except Exception as e:
        print(f"ERROR IN CREATING MONTHLY SUMMARY: {str(e)}")
        print(traceback.format_exc())
        return
    
    # Step 4: Apply corrections
    try:
        print_checkpoint("STEP 4: Applying corrections to known data issues")
        corrected_summary = correct_known_data_issues(monthly_summary)
        print_checkpoint("Corrections applied successfully")
    except Exception as e:
        print(f"ERROR IN APPLYING CORRECTIONS: {str(e)}")
        print(traceback.format_exc())
        return
    
    # Step 5: Save monthly summary
    try:
        print_checkpoint("STEP 5: Saving monthly summary")
        summary_file = save_monthly_summary(corrected_summary)
        print_checkpoint(f"Monthly summary saved to {summary_file}")
    except Exception as e:
        print(f"ERROR IN SAVING MONTHLY SUMMARY: {str(e)}")
        print(traceback.format_exc())
        return
    
    # Step 6: Generate forecast
    try:
        print_checkpoint("STEP 6: Generating forecast")
        forecast_periods = 12  # Forecast for 1 year
        lookback_years = 5     # Use 5 years of data
        forecast_methods = ['simple', 'ets']  # Use both simple and ETS methods
        
        print_checkpoint("Calling generate_business_forecast_from_summary...")
        forecast_data = generate_business_forecast_from_summary(
            corrected_summary, 
            forecast_periods=forecast_periods,
            lookback_years=lookback_years,
            methods=forecast_methods
        )
        print_checkpoint(f"Forecast generated successfully. Shape: {forecast_data.shape}")
        print(f"Last 15 rows of forecast data (includes historical + forecast):\n{forecast_data.tail(15)}")
    except Exception as e:
        print(f"ERROR IN GENERATING FORECAST: {str(e)}")
        print(traceback.format_exc())
        return
    
    # Step 7: Save forecast
    try:
        print_checkpoint("STEP 7: Saving forecast data")
        forecast_file = save_forecast_data(forecast_data)
        print_checkpoint(f"Forecast saved to {forecast_file}")
    except Exception as e:
        print(f"ERROR IN SAVING FORECAST: {str(e)}")
        print(traceback.format_exc())
        return
    
    print_checkpoint("All steps completed successfully!")

if __name__ == "__main__":
    diagnostic_run()