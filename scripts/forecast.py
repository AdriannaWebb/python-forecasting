# Add this to scripts/forecast.py (keep all other existing functions)
# Add these imports to the top of scripts/forecast.py
import pandas as pd
import numpy as np
from datetime import datetime
from scripts.logging_setup import logger

def generate_business_forecast_from_summary(monthly_summary_df, forecast_periods=12, lookback_years=5, method='simple'):
    """
    Generate forecasts for business joins and drops using the monthly summary data
    
    Parameters:
    - monthly_summary_df: DataFrame containing monthly business summary
    - forecast_periods: Number of periods (months) to forecast
    - lookback_years: Number of years of historical data to use
    - method: Forecasting method ('simple', 'sarima', 'seasonal', 'ensemble')
    
    Returns:
    - DataFrame with historical and forecast data
    """
    try:
        from scripts.transform import prepare_summary_for_forecast
        
        logger.info(f"Generating business forecast for {forecast_periods} months using {method} method")
        
        # Prepare data for forecasting
        data_df = prepare_summary_for_forecast(monthly_summary_df, lookback_years)
        
        # Create a copy for forecasting
        forecast_df = data_df.copy()
        
        # Get the last date in our data
        last_date = forecast_df['year_month'].max()
        
        # Create forecast dates
        forecast_dates = pd.date_range(
            start=last_date + pd.DateOffset(months=1),
            periods=forecast_periods,
            freq='MS'
        )
        
        # Create empty rows for forecast periods
        forecast_rows = pd.DataFrame({
            'year_month': forecast_dates,
            'year': [d.year for d in forecast_dates],
            'month': [d.month for d in forecast_dates],
            'month_name': [d.strftime('%b') for d in forecast_dates],
            'is_forecast': True
        })
        
        # Apply the forecasting method
        if method == 'simple':
            # Simple method: average of the same month over past years
            for i, row in forecast_rows.iterrows():
                month = row['month']
                # Get historical data for the same month
                same_month_data = forecast_df[forecast_df['month'] == month]
                
                if len(same_month_data) > 0:
                    # Calculate average joins and drops for this month
                    avg_joins = same_month_data['new_joins'].mean()
                    avg_drops = same_month_data['new_drops'].mean()
                    
                    # Apply a simple trend adjustment based on recent data
                    recent_data = forecast_df.tail(12)  # Last year's data
                    if len(recent_data) > 0:
                        recent_avg_joins = recent_data['new_joins'].mean()
                        recent_avg_drops = recent_data['new_drops'].mean()
                        
                        all_avg_joins = forecast_df['new_joins'].mean()
                        all_avg_drops = forecast_df['new_drops'].mean()
                        
                        # Calculate trend factors
                        join_trend_factor = recent_avg_joins / all_avg_joins if all_avg_joins > 0 else 1
                        drop_trend_factor = recent_avg_drops / all_avg_drops if all_avg_drops > 0 else 1
                        
                        # Apply trend adjustment
                        avg_joins = avg_joins * join_trend_factor
                        avg_drops = avg_drops * drop_trend_factor
                    
                    # Round to integers
                    forecast_rows.at[i, 'new_joins'] = round(avg_joins)
                    forecast_rows.at[i, 'new_drops'] = round(avg_drops)
                else:
                    # If no historical data for this month, use overall average
                    forecast_rows.at[i, 'new_joins'] = round(forecast_df['new_joins'].mean())
                    forecast_rows.at[i, 'new_drops'] = round(forecast_df['new_drops'].mean())
        
        # For now, we'll only implement the simple method
        # More advanced methods can be added later
        else:
            logger.warning(f"Method '{method}' not implemented yet. Using 'simple' method instead.")
            # Call this function recursively with simple method
            return generate_business_forecast_from_summary(
                monthly_summary_df, 
                forecast_periods=forecast_periods,
                lookback_years=lookback_years,
                method='simple'
            )
        
        # Calculate net change for forecast periods
        forecast_rows['net_change'] = forecast_rows['new_joins'] - forecast_rows['new_drops']
        
        # Get the last actual active business count
        last_actual_count = forecast_df['active_businesses'].iloc[-1]
        
        # Calculate active businesses for each forecast period
        prev_active = last_actual_count
        for i, row in forecast_rows.iterrows():
            current_active = prev_active + row['net_change']
            forecast_rows.at[i, 'active_businesses'] = current_active
            prev_active = current_active
        
        # Combine historical and forecast data
        # First, mark historical data as not forecast
        forecast_df['is_forecast'] = False
        
        # Combine and sort by date
        combined_df = pd.concat([forecast_df, forecast_rows], ignore_index=True)
        combined_df = combined_df.sort_values('year_month').reset_index(drop=True)
        
        logger.info(f"Forecast generated successfully for {forecast_periods} months")
        return combined_df
    except Exception as e:
        logger.error(f"Error generating business forecast from summary: {e}")
        raise