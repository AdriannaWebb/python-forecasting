# scripts/forecast.py
import pandas as pd
import numpy as np
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.statespace.sarimax import SARIMAX
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
import datetime
from scripts.logging_setup import logger

def time_based_train_test_split(df, test_size=6):
    """
    Split time series data into train and test sets based on time
    Last test_size months will be used for testing
    """
    if len(df) <= test_size:
        return df, pd.DataFrame(columns=df.columns)
    
    train = df.iloc[:-test_size].copy()
    test = df.iloc[-test_size:].copy()
    
    return train, test

def evaluate_forecast_accuracy(actual, predicted):
    """
    Calculate forecast accuracy metrics
    """
    mae = mean_absolute_error(actual, predicted)
    rmse = np.sqrt(mean_squared_error(actual, predicted))
    mape = np.mean(np.abs((actual - predicted) / np.maximum(1, actual))) * 100
    
    return {
        'MAE': mae,
        'RMSE': rmse,
        'MAPE': mape
    }

def forecast_sarima(df, column, periods=12, seasonal_period=12):
    """
    Forecast using SARIMA model with automatic parameter selection
    """
    try:
        # Prepare data
        y = df[column].values
        
        # Try different SARIMA parameters and choose best model
        best_aic = float('inf')
        best_model = None
        best_params = None
        
        # Simple grid search for SARIMA parameters
        p_values = [0, 1, 2]
        d_values = [0, 1]
        q_values = [0, 1]
        
        for p in p_values:
            for d in d_values:
                for q in q_values:
                    try:
                        model = SARIMAX(
                            y, 
                            order=(p, d, q), 
                            seasonal_order=(1, 1, 0, seasonal_period),
                            enforce_stationarity=False,
                            enforce_invertibility=False
                        )
                        
                        model_fit = model.fit(disp=False, maxiter=200)
                        
                        if model_fit.aic < best_aic:
                            best_aic = model_fit.aic
                            best_model = model_fit
                            best_params = (p, d, q)
                    except:
                        continue
        
        if best_model is None:
            logger.warning(f"Could not find suitable SARIMA model for {column}, using default parameters")
            model = SARIMAX(
                y, 
                order=(1, 1, 1), 
                seasonal_order=(1, 1, 0, seasonal_period),
                enforce_stationarity=False,
                enforce_invertibility=False
            )
            best_model = model.fit(disp=False)
        
        # Forecast
        forecast = best_model.forecast(steps=periods)
        forecast = np.maximum(forecast, 0)  # Ensure non-negative values
        
        # Create forecast dataframe
        last_date = df['year_month'].iloc[-1]
        forecast_dates = pd.date_range(
            start=last_date + pd.DateOffset(months=1),
            periods=periods,
            freq='MS'
        )
        
        forecast_df = pd.DataFrame({
            'year_month': forecast_dates,
            f'{column}_forecast': forecast.round().astype(int)
        })
        
        logger.info(f"SARIMA forecast completed for {column} with parameters {best_params}")
        return forecast_df
    except Exception as e:
        logger.error(f"Error in SARIMA forecasting for {column}: {e}")
        raise

def forecast_with_seasonal_decomposition(df, column, periods=12):
    """
    Forecast using seasonal decomposition and trend extrapolation
    """
    try:
        # Set the index to year_month for decomposition
        ts_df = df.set_index('year_month')[[column]].copy()
        
        # Perform seasonal decomposition
        decomposition = seasonal_decompose(ts_df, model='additive', period=12)
        
        # Extract components
        trend = decomposition.trend
        seasonal = decomposition.seasonal
        residual = decomposition.resid
        
        # Fill NaN values in trend with the closest non-NaN values
        trend = trend.fillna(method='ffill').fillna(method='bfill')
        
        # Get the last trend value and calculate trend slope
        last_trend = trend.iloc[-12:].mean()
        trend_slope = (trend.iloc[-1] - trend.iloc[-13]) / 12
        
        # Create forecast dates
        last_date = df['year_month'].iloc[-1]
        forecast_dates = pd.date_range(
            start=last_date + pd.DateOffset(months=1),
            periods=periods,
            freq='MS'
        )
        
        forecast_df = pd.DataFrame({'year_month': forecast_dates})
        forecast_values = []
        
        for i in range(periods):
            # Project trend
            future_trend = last_trend + trend_slope * (i + 1)
            
            # Get seasonal component for this month (from the previous year)
            month = forecast_dates[i].month
            seasonal_factor = seasonal.iloc[(seasonal.index.month == month)].values.mean()
            
            # Add projected trend and seasonal component
            forecast_value = future_trend + seasonal_factor
            forecast_values.append(max(0, forecast_value))  # Ensure non-negative
        
        forecast_df[f'{column}_forecast'] = np.array(forecast_values).round().astype(int)
        
        logger.info(f"Seasonal decomposition forecast completed for {column}")
        return forecast_df
    except Exception as e:
        logger.error(f"Error in seasonal decomposition forecasting for {column}: {e}")
        # Fall back to SARIMA if seasonal decomposition fails
        logger.info(f"Falling back to SARIMA for {column}")
        return forecast_sarima(df, column, periods)

def combine_forecasts(data_df, join_forecast_df, drop_forecast_df):
    """
    Combine forecasts into a single dataframe with actual and forecast values
    """
    try:
        # Create the combined dataframe starting with historical data
        combined_df = data_df.copy()
        
        # Add forecast data
        forecast_df = pd.merge(
            join_forecast_df,
            drop_forecast_df,
            on='year_month',
            how='outer'
        )
        
        combined_df = pd.concat([combined_df, forecast_df], ignore_index=True)
        
        # Calculate net change and cumulative totals for forecast period
        last_actual_cumulative = combined_df.loc[combined_df['year_month'] == data_df['year_month'].max(), 'cumulative_total'].values[0]
        
        # Fill forecast periods
        forecast_start_idx = len(data_df)
        forecast_rows = combined_df.iloc[forecast_start_idx:]
        
        for i, row in forecast_rows.iterrows():
            idx = combined_df.index[i]
            join_forecast = row['join_count_forecast']
            drop_forecast = row['drop_count_forecast']
            
            # Add forecasted values to the main dataframe
            combined_df.at[idx, 'join_count'] = join_forecast
            combined_df.at[idx, 'drop_count'] = drop_forecast
            combined_df.at[idx, 'net_change'] = join_forecast - drop_forecast
            
            # Update cumulative total
            if i == forecast_start_idx:
                prev_cum_total = last_actual_cumulative
            else:
                prev_cum_total = combined_df.at[combined_df.index[i-1], 'cumulative_total']
                
            combined_df.at[idx, 'cumulative_total'] = prev_cum_total + (join_forecast - drop_forecast)
        
        # Add is_forecast flag
        combined_df['is_forecast'] = False
        combined_df.loc[combined_df.index >= forecast_start_idx, 'is_forecast'] = True
        
        # Add date parts
        combined_df['year'] = combined_df['year_month'].dt.year
        combined_df['month'] = combined_df['year_month'].dt.month
        combined_df['month_name'] = combined_df['year_month'].dt.strftime('%b')
        
        logger.info(f"Combined forecasts with historical data. Total rows: {len(combined_df)}")
        return combined_df
    except Exception as e:
        logger.error(f"Error combining forecasts: {e}")
        raise

def generate_business_forecast(df, forecast_periods=12, method='seasonal'):
    """
    Generate forecasts for business joins and drops
    
    Parameters:
    - df: The time series dataframe with historical data
    - forecast_periods: Number of periods (months) to forecast
    - method: Forecasting method ('sarima', 'seasonal', or 'ensemble')
    
    Returns:
    - combined_df: Combined dataframe with historical and forecast data
    """
    try:
        logger.info(f"Generating business forecast for {forecast_periods} months using {method} method")
        
        # Create copy of data to avoid modifying original
        data_df = df.copy()
        
        # Generate forecasts based on selected method
        if method == 'sarima':
            join_forecast_df = forecast_sarima(data_df, 'join_count', forecast_periods)
            drop_forecast_df = forecast_sarima(data_df, 'drop_count', forecast_periods)
        elif method == 'seasonal':
            join_forecast_df = forecast_with_seasonal_decomposition(data_df, 'join_count', forecast_periods)
            drop_forecast_df = forecast_with_seasonal_decomposition(data_df, 'drop_count', forecast_periods)
        elif method == 'ensemble':
            # Ensemble method - average of SARIMA and seasonal decomposition
            join_sarima = forecast_sarima(data_df, 'join_count', forecast_periods)
            join_seasonal = forecast_with_seasonal_decomposition(data_df, 'join_count', forecast_periods)
            drop_sarima = forecast_sarima(data_df, 'drop_count', forecast_periods)
            drop_seasonal = forecast_with_seasonal_decomposition(data_df, 'drop_count', forecast_periods)
            
            # Average the forecasts
            join_forecast_df = join_sarima.copy()
            join_forecast_df['join_count_forecast'] = (
                (join_sarima['join_count_forecast'] + join_seasonal['join_count_forecast']) / 2
            ).round().astype(int)
            
            drop_forecast_df = drop_sarima.copy()
            drop_forecast_df['drop_count_forecast'] = (
                (drop_sarima['drop_count_forecast'] + drop_seasonal['drop_count_forecast']) / 2
            ).round().astype(int)
        else:
            raise ValueError(f"Unknown forecasting method: {method}")
        
        # Combine forecasts
        combined_df = combine_forecasts(data_df, join_forecast_df, drop_forecast_df)
        
        logger.info("Forecast generation completed successfully")
        return combined_df
    except Exception as e:
        logger.error(f"Error generating business forecast: {e}")
        raise