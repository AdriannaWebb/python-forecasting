# scripts/transform.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from scripts.logging_setup import logger

def clean_business_data(df):
    """Clean the business data by removing rows without join dates"""
    try:
        # Remove entries without date_accredited
        cleaned_df = df.dropna(subset=['date_accredited'])
        
        original_count = len(df)
        cleaned_count = len(cleaned_df)
        removed_count = original_count - cleaned_count
        
        logger.info(f"Cleaned business data: removed {removed_count} rows without join dates")
        logger.info(f"Remaining records: {cleaned_count}")
        
        return cleaned_df
    except Exception as e:
        logger.error(f"Error cleaning business data: {e}")
        raise

def filter_data_by_date_range(df, lookback_years=5):
    """
    Filter data to only include records within the specified lookback period
    """
    try:
        today = pd.to_datetime(datetime.now())
        cutoff_date = today - pd.DateOffset(years=lookback_years)
        
        # Convert dates to datetime if they aren't already
        df['date_accredited'] = pd.to_datetime(df['date_accredited'])
        
        # Filter to recent data only
        filtered_df = df[df['date_accredited'] >= cutoff_date]
        
        logger.info(f"Filtered to data from the past {lookback_years} years")
        logger.info(f"Original record count: {len(df)}, Filtered record count: {len(filtered_df)}")
        
        return filtered_df
    except Exception as e:
        logger.error(f"Error filtering data by date range: {e}")
        raise

def aggregate_monthly_data(df):
    """
    Aggregate data to monthly level for time series forecasting
    Returns two dataframes: one for joins and one for drops
    """
    try:
        # Convert dates to datetime if they aren't already
        df['date_accredited'] = pd.to_datetime(df['date_accredited'])
        if 'date_dropped' in df.columns:
            df['date_dropped'] = pd.to_datetime(df['date_dropped'])
        
        # Create monthly join counts
        df_joins = df.copy()
        df_joins['year_month'] = df_joins['date_accredited'].dt.to_period('M')
        monthly_joins = df_joins.groupby('year_month').size().reset_index(name='join_count')
        monthly_joins['year_month'] = monthly_joins['year_month'].dt.to_timestamp()
        
        # Create monthly drop counts (exclude NaN drop dates)
        df_drops = df.dropna(subset=['date_dropped']).copy()
        df_drops['year_month'] = df_drops['date_dropped'].dt.to_period('M')
        monthly_drops = df_drops.groupby('year_month').size().reset_index(name='drop_count')
        monthly_drops['year_month'] = monthly_drops['year_month'].dt.to_timestamp()
        
        logger.info(f"Created monthly aggregates: {len(monthly_joins)} months of join data")
        logger.info(f"Created monthly aggregates: {len(monthly_drops)} months of drop data")
        
        return monthly_joins, monthly_drops
    except Exception as e:
        logger.error(f"Error aggregating monthly data: {e}")
        raise

def create_time_series_df(monthly_joins, monthly_drops, start_date=None, end_date=None):
    """
    Create a complete time series dataframe with joins and drops
    Fill in missing months with zeros
    """
    try:
        # If no date range specified, use min and max dates from the data
        if start_date is None:
            join_min = monthly_joins['year_month'].min()
            drop_min = monthly_drops['year_month'].min() if not monthly_drops.empty else pd.NaT
            dates = [join_min, drop_min]
            start_date = min(d for d in dates if not pd.isna(d))
        
        if end_date is None:
            join_max = monthly_joins['year_month'].max() 
            drop_max = monthly_drops['year_month'].max() if not monthly_drops.empty else pd.NaT
            dates = [join_max, drop_max]
            end_date = max(d for d in dates if not pd.isna(d))
        
        # Create a complete date range at month level
        date_range = pd.date_range(start=start_date, end=end_date, freq='MS')
        time_series_df = pd.DataFrame({'year_month': date_range})
        
        # Merge join counts
        time_series_df = pd.merge(
            time_series_df, monthly_joins, 
            on='year_month', how='left'
        )
        
        # Merge drop counts
        time_series_df = pd.merge(
            time_series_df, monthly_drops,
            on='year_month', how='left'
        )
        
        # Fill NaN values with 0
        time_series_df['join_count'] = time_series_df['join_count'].fillna(0).astype(int)
        time_series_df['drop_count'] = time_series_df['drop_count'].fillna(0).astype(int)
        
        # Add net change column
        time_series_df['net_change'] = time_series_df['join_count'] - time_series_df['drop_count']
        
        # Calculate cumulative totals (running business count)
        time_series_df['cumulative_total'] = time_series_df['net_change'].cumsum()
        
        logger.info(f"Created complete time series dataframe with {len(time_series_df)} months")
        return time_series_df
    except Exception as e:
        logger.error(f"Error creating time series dataframe: {e}")
        raise

def add_date_features(df):
    """
    Add date-related features that might be useful for forecasting
    """
    try:
        # Add month and year columns
        df['month'] = df['year_month'].dt.month
        df['year'] = df['year_month'].dt.year
        
        # Add quarter
        df['quarter'] = df['year_month'].dt.quarter
        
        # Add season (Northern Hemisphere)
        season_map = {
            1: 'Winter', 2: 'Winter', 3: 'Spring', 
            4: 'Spring', 5: 'Spring', 6: 'Summer',
            7: 'Summer', 8: 'Summer', 9: 'Fall', 
            10: 'Fall', 11: 'Fall', 12: 'Winter'
        }
        df['season'] = df['month'].map(season_map)
        
        logger.info("Added date features to time series dataframe")
        return df
    except Exception as e:
        logger.error(f"Error adding date features: {e}")
        raise
def create_monthly_business_summary(df, start_year=2010):
    """
    Create a monthly summary of business data from start_year to current month
    - Count of active businesses at start of month
    - Count of new businesses that joined in the month
    - Count of businesses that dropped in the month
    
    Returns a dataframe with these metrics for each month
    """
    try:
        # Ensure dates are in datetime format
        df['date_accredited'] = pd.to_datetime(df['date_accredited'])
        if 'date_dropped' in df.columns:
            df['date_dropped'] = pd.to_datetime(df['date_dropped'])
        
        # Get current date and create date range
        current_date = datetime.now()
        # Go to first day of current month
        current_month_start = pd.Timestamp(year=current_date.year, month=current_date.month, day=1)
        # Create date range from start_year to current month
        start_date = pd.Timestamp(year=start_year, month=1, day=1)
        date_range = pd.date_range(start=start_date, end=current_month_start, freq='MS')
        
        # Create empty dataframe to hold the summary
        summary_df = pd.DataFrame({'year_month': date_range})
        
        # Initialize columns
        summary_df['active_businesses'] = 0
        summary_df['new_joins'] = 0
        summary_df['new_drops'] = 0
        
        # Get monthly joins
        joins_df = df.copy()
        joins_df['join_month'] = joins_df['date_accredited'].dt.to_period('M')
        monthly_joins = joins_df.groupby('join_month').size().reset_index(name='count')
        monthly_joins['join_month'] = monthly_joins['join_month'].dt.to_timestamp()
        
        # Get monthly drops (for businesses that have dropped)
        drops_df = df.dropna(subset=['date_dropped']).copy()
        drops_df['drop_month'] = drops_df['date_dropped'].dt.to_period('M')
        monthly_drops = drops_df.groupby('drop_month').size().reset_index(name='count')
        monthly_drops['drop_month'] = monthly_drops['drop_month'].dt.to_timestamp()
        
        # Merge joins and drops into summary
        summary_df = pd.merge(
            summary_df, 
            monthly_joins.rename(columns={'join_month': 'year_month', 'count': 'new_joins'}),
            on='year_month', how='left'
        )
        
        summary_df = pd.merge(
            summary_df,
            monthly_drops.rename(columns={'drop_month': 'year_month', 'count': 'new_drops'}),
            on='year_month', how='left'
        )
        
        # Fill NaN values with 0
        summary_df['new_joins'] = summary_df['new_joins'].fillna(0).astype(int)
        summary_df['new_drops'] = summary_df['new_drops'].fillna(0).astype(int)
        
        # Calculate active businesses for each month
        for i, row in summary_df.iterrows():
            month_start = row['year_month']
            month_end = month_start + pd.DateOffset(months=1) - pd.DateOffset(days=1)
            
            # Count businesses that joined before or during this month and either haven't dropped
            # or dropped after this month
            active_count = len(df[
                (df['date_accredited'] <= month_end) & 
                ((df['date_dropped'].isna()) | (df['date_dropped'] > month_start))
            ])
            
            summary_df.at[i, 'active_businesses'] = active_count
        
        # Format columns for better readability
        summary_df['year'] = summary_df['year_month'].dt.year
        summary_df['month'] = summary_df['year_month'].dt.month
        summary_df['month_name'] = summary_df['year_month'].dt.strftime('%b')
        
        # Reorder columns
        summary_df = summary_df[[
            'year_month', 'year', 'month', 'month_name', 
            'active_businesses', 'new_joins', 'new_drops'
        ]]
        
        logger.info(f"Created monthly business summary from {start_year} to {current_date.year}-{current_date.month}")
        return summary_df
    except Exception as e:
        logger.error(f"Error creating monthly business summary: {e}")
        raise
