# test_extract.py
from scripts.extract import extract_business
from scripts.logging_setup import logger

def main():
    try:
        # Test extracting data from CashReceipts
        logger.info("Testing database connection and extraction...")
        df = extract_business()
        
        # Print the first few rows and column names to verify
        print("\nColumns in Business table:")
        print(df.columns.tolist())
        
        print("\nFirst 5 rows of data:")
        print(df.head(5))
        
        print(f"\nTotal rows extracted: {len(df)}")
        
        logger.info("Extract test completed successfully")
    except Exception as e:
        logger.error(f"Test failed: {e}")

if __name__ == "__main__":
    main()