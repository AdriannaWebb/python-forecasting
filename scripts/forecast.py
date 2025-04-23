# Add this to scripts/forecast.py (keep all other existing functions)
# Add these imports to the top of scripts/forecast.py
import pandas as pd
import numpy as np
from datetime import datetime
from scripts.logging_setup import logger

def generate_business_forecast_from_summary(monthly_summary_df, forecast_periods=12, lookback_years=5, methods=None):
    """
    Generate forecasts for business joins and drops using the monthly summary data
    
    Parameters:
    - monthly_summary_df: DataFrame containing monthly business summary
    - forecast_periods: Number of periods (months) to forecast
    - lookback_years: Number of years of historical data to use
    - methods: List of forecasting methods to use ('simple', 'ets', etc.)
    
    Returns:
    - DataFrame with historical and forecast data
    """
    try:
        from scripts.transform import prepare_summary_for_forecast
        
        # Default methods
        if methods is None:
            methods = ['simple', 'ets']
        
        logger.info(f"Generating business forecast for {forecast_periods} months using methods: {', '.join(methods)}")
        
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
        
        # Apply the simple forecasting method if requested
        if 'simple' in methods:
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
                    forecast_rows.at[i, 'simple_joins'] = round(avg_joins)
                    forecast_rows.at[i, 'simple_drops'] = round(avg_drops)
                else:
                    # If no historical data for this month, use overall average
                    forecast_rows.at[i, 'simple_joins'] = round(forecast_df['new_joins'].mean())
                    forecast_rows.at[i, 'simple_drops'] = round(forecast_df['new_drops'].mean())
        
        # Apply the ETS forecasting method if requested
        if 'ets' in methods:
            # Generate ETS forecasts
            ets_joins, ets_drops = generate_ets_forecast(data_df, forecast_periods)
            
            # Add ETS forecasts to forecast rows
            for i in range(len(forecast_rows)):
                if i < len(ets_joins):
                    forecast_rows.at[i, 'ets_joins'] = ets_joins[i]
                if i < len(ets_drops):
                    forecast_rows.at[i, 'ets_drops'] = ets_drops[i]
        
        # Calculate net change and active businesses for each method
        method_columns = {}
        for method in methods:
            join_col = f'{method}_joins'
            drop_col = f'{method}_drops'
            
            if join_col in forecast_rows.columns and drop_col in forecast_rows.columns:
                # Calculate net change
                net_col = f'{method}_net_change'
                forecast_rows[net_col] = forecast_rows[join_col] - forecast_rows[drop_col]
                
                # Get the last actual active business count
                last_actual_count = forecast_df['active_businesses'].iloc[-1]
                
                # Calculate active businesses for each forecast period
                active_col = f'{method}_active'
                forecast_rows[active_col] = 0
                prev_active = last_actual_count
                
                for i, row in forecast_rows.iterrows():
                    current_active = prev_active + row[net_col]
                    forecast_rows.at[i, active_col] = current_active
                    prev_active = current_active
                
                # Store column names for this method
                method_columns[method] = {
                    'joins': join_col,
                    'drops': drop_col,
                    'net_change': net_col,
                    'active': active_col
                }
        
        # For backward compatibility, copy the first method columns to the standard column names
        default_method = methods[0]
        if default_method in method_columns:
            cols = method_columns[default_method]
            forecast_rows['new_joins'] = forecast_rows[cols['joins']]
            forecast_rows['new_drops'] = forecast_rows[cols['drops']]
            forecast_rows['net_change'] = forecast_rows[cols['net_change']]
            forecast_rows['active_businesses'] = forecast_rows[cols['active']]
        
        # Combine historical and forecast data
        # First, mark historical data as not forecast
        forecast_df['is_forecast'] = False
        
        # Combine and sort by date
        combined_df = pd.concat([forecast_df, forecast_rows], ignore_index=True)
        combined_df = combined_df.sort_values('year_month').reset_index(drop=True)
        
        logger.info(f"Forecast generated successfully for {forecast_periods} months using methods: {', '.join(methods)}")
        return combined_df
    except Exception as e:
        logger.error(f"Error generating business forecast from summary: {e}")
        raise

def generate_ets_forecast(data_df, forecast_periods=12):
    """
    Generate forecasts using exponential smoothing (ETS)
    
    Parameters:
    - data_df: DataFrame containing historical data with 'new_joins' and 'new_drops' columns
    - forecast_periods: Number of periods (months) to forecast
    
    Returns:
    - Tuple of (join_forecast, drop_forecast) arrays for the forecast periods
    """
    try:
        from statsmodels.tsa.holtwinters import ExponentialSmoothing
        
        logger.info("Generating ETS forecast...")
        
        # Prepare forecasts for joins and drops
        join_forecast = []
        drop_forecast = []
        
        # Train ETS model for joins
        join_series = data_df['new_joins'].values
        
        # Find optimal parameters for joins
        best_joins_model = None
        best_joins_mse = float('inf')
        
        # Try different seasonal periods (s) for joins
        for s in [3, 4, 6, 12]:
            if len(join_series) >= s * 2:  # Ensure enough data for estimation
                try:
                    # Create model with additive seasonality and trend
                    model = ExponentialSmoothing(
                        join_series,
                        seasonal_periods=s,
                        trend='add',
                        seasonal='add',
                        damped=True
                    ).fit(optimized=True)
                    
                    # Get in-sample predictions
                    predictions = model.predict(start=0, end=len(join_series)-1)
                    
                    # Calculate MSE
                    mse = np.mean((join_series - predictions)**2)
                    
                    # Update best model if this one is better
                    if mse < best_joins_mse:
                        best_joins_mse = mse
                        best_joins_model = model
                except:
                    # Skip if model fitting fails
                    continue
        
        # If no model was successfully fit, use simpler model
        if best_joins_model is None:
            best_joins_model = ExponentialSmoothing(
                join_series,
                trend='add',
                seasonal=None,
                damped=True
            ).fit(optimized=True)
        
        # Generate forecasts for joins
        join_forecast = best_joins_model.forecast(forecast_periods).values
        
        # Train ETS model for drops
        drop_series = data_df['new_drops'].values
        
        # Find optimal parameters for drops
        best_drops_model = None
        best_drops_mse = float('inf')
        
        # Try different seasonal periods (s) for drops
        for s in [3, 4, 6, 12]:
            if len(drop_series) >= s * 2:  # Ensure enough data for estimation
                try:
                    # Create model with additive seasonality and trend
                    model = ExponentialSmoothing(
                        drop_series,
                        seasonal_periods=s,
                        trend='add',
                        seasonal='add',
                        damped=True
                    ).fit(optimized=True)
                    
                    # Get in-sample predictions
                    predictions = model.predict(start=0, end=len(drop_series)-1)
                    
                    # Calculate MSE
                    mse = np.mean((drop_series - predictions)**2)
                    
                    # Update best model if this one is better
                    if mse < best_drops_mse:
                        best_drops_mse = mse
                        best_drops_model = model
                except:
                    # Skip if model fitting fails
                    continue
        
        # If no model was successfully fit, use simpler model
        if best_drops_model is None:
            best_drops_model = ExponentialSmoothing(
                drop_series,
                trend='add',
                seasonal=None,
                damped=True
            ).fit(optimized=True)
        
        # Generate forecasts for drops
        drop_forecast = best_drops_model.forecast(forecast_periods).values
        
        # Round to integers
        join_forecast = np.round(join_forecast).astype(int)
        drop_forecast = np.round(drop_forecast).astype(int)
        
        # Ensure non-negative values
        join_forecast = np.maximum(join_forecast, 0)
        drop_forecast = np.maximum(drop_forecast, 0)
        
        logger.info("ETS forecast generated successfully")
        
        return join_forecast, drop_forecast
    except Exception as e:
        logger.error(f"Error generating ETS forecast: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise   