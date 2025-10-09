import openmeteo_requests

import pandas as pd
import datetime
import numpy as np

import requests_cache
from retry_requests import retry

def get_historical_dfs():
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    yesterday = str(datetime.date.today() - datetime.timedelta(days=1))

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": [42.3584, 40.7608, 47.6062],
        "longitude": [-71.0598, -111.8911, -122.3321],
        "start_date": "2025-07-01",
        "end_date": yesterday,
        "daily": "precipitation_sum",
        "timezone": "America/New_York",
    }
    responses = openmeteo.weather_api(url, params=params)

    cities = ['boston', 'slc', 'seattle']
    dfs = {}

    # Process 3 locations
    for i in range(len(responses)):
        response = responses[i]
        
        # Process daily data. The order of variables needs to be the same as requested.
        daily = response.Daily()
        daily_precipitation_sum = daily.Variables(0).ValuesAsNumpy()
        
        daily_data = {"date": pd.date_range(
            start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = daily.Interval()),
            inclusive = "left"
        )}
        
        # Turn 'actual' column into a boolean
        daily_data["actual"] = daily_precipitation_sum > 0

        daily_dataframe = pd.DataFrame(data = daily_data)
        dfs[cities[i]] = daily_dataframe

    return dfs

def update_csv():

    for i in range(2):

        # Get historical data with our other function
        dfs = get_historical_dfs()

        for key, value in dfs.items():
            FILEPATH = ''
            city = key

            # Ensure both columns have just the date as the index
            value["date"] = pd.to_datetime(value["date"]).dt.date
            value = value.set_index('date')

            if i == 0:
                # Read in other csv (NWS)
                FILEPATH = f"{city}_nws_forecast_log.csv"
                df = pd.read_csv(FILEPATH, parse_dates=["date"])
            else:
                # Read in other csv (Open-Meteo)
                FILEPATH = f"{city}_precip_forecast_log.csv"
                df = pd.read_csv(FILEPATH, parse_dates=["date"])


            df = df.set_index('date')

            # Just get the old data for the rows in the other df
            start_date = df.index[0].date()
            value = value[value.index >= start_date]

            # Combine dataframes
            # df['actual'] = np.nan
            df = df.combine_first(value)

            # Get columns into the right order
            cols = ["actual", "0_days_out", "1_days_out", "2_days_out", "3_days_out", "4_days_out", "5_days_out", 
                            "6_days_out", "7_days_out", "8_days_out", "9_days_out", "10_days_out", "11_days_out", 
                            "12_days_out", "13_days_out", "14_days_out", "15_days_out"]
            
            if i == 0:
                df = df[[cols][:8]]
            else:
                df = df[[cols]]

            df.sort_index().to_csv(FILEPATH)



if __name__ == "__main__":
    update_csv()
