import openmeteo_requests

import numpy as np
import pandas as pd
import requests_cache
from retry_requests import retry
import os
import datetime


# CSV file to log data
FILEPATH = "precip_forecast_log.csv"

def get_daily_data():

    today = datetime.date.today()
    three_days_ago = str(today - datetime.timedelta(days=4))
    dfs = {}
    latitudes = [42.3584, 40.7608, 47.6062]
    longitudes = [-71.0598, -111.8911, -122.3321]

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    archive_responses = []
    for i in range(3):
        archive_url = "https://archive-api.open-meteo.com/v1/archive"
        archive_params = {
            "latitude": latitudes[i],
            "longitude": longitudes[i],
            "start_date": three_days_ago,
            "end_date": three_days_ago,
            "daily": "precipitation_sum",
            "timezone": "America/New_York"
        }

        archive_response = openmeteo.weather_api(archive_url, params=archive_params)

        archive_responses += archive_response

    forecast_responses = []
    for i in range(3):
        forecast_url = "https://api.open-meteo.com/v1/forecast"
        forecast_params = {
            "latitude": latitudes[i],
            "longitude": longitudes[i],
            "daily": "precipitation_probability_max",
            "timezone": "America/New_York",
            "past_days": 3,
            "forecast_days": 16
        }
        forecast_response = openmeteo.weather_api(forecast_url, params=forecast_params)

        forecast_responses += forecast_response


    for i in range(3):
        city = ["boston", "slc", "seattle"][i]

        ### ARCHIVE DATA
        archive_response = archive_responses[i]
        # Process daily data. The order of variables needs to be the same as requested.
        archive_daily = archive_response.Daily()
        daily_precipitation_sum = archive_daily.Variables(0).ValuesAsNumpy()

        archive_daily_data = {"date": pd.date_range(
            start = pd.to_datetime(archive_daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(archive_daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = archive_daily.Interval()),
            inclusive = "left"
        )}

        archive_daily_data["precipitation_sum"] = daily_precipitation_sum
        archive_daily_dataframe = pd.DataFrame(data = archive_daily_data)


        ### FORECAST DATA
        # Process daily data. The order of variables needs to be the same as requested.
        forecast_response = forecast_responses[i]
        forecast_daily = forecast_response.Daily()
        daily_precipitation_probability_max = forecast_daily.Variables(0).ValuesAsNumpy()

        forecast_daily_data = {"date": pd.date_range(
            start = pd.to_datetime(forecast_daily.Time(), unit = "s", utc = True),
            end = pd.to_datetime(forecast_daily.TimeEnd(), unit = "s", utc = True),
            freq = pd.Timedelta(seconds = forecast_daily.Interval()),
            inclusive = "left"
        )}

        forecast_daily_data["precipitation_probability_max"] = daily_precipitation_probability_max

        forecast_daily_dataframe = pd.DataFrame(data = forecast_daily_data)

        df = pd.merge(forecast_daily_dataframe, archive_daily_dataframe, how="outer", on="date")

        dfs[city] = df
    
    return dfs

# Append to CSV
def log_forecast():
    dfs = get_daily_data()

    for city, df in dfs.items():

        df["date"] = pd.to_datetime(df["date"]).dt.date

        today = datetime.date.today()

        updated_rows = []

        for i, row in df.iterrows():
            forecast_date = row["date"]
            precip = row["precipitation_probability_max"]

            # Determine what kind of column this is
            delta = (forecast_date - today).days

            if delta == -3:
                column = "actual"
                precip = row["precipitation_sum"]
            elif delta >= 0:
                column = f"{delta}_days_out"
            elif delta > -3 and delta < 0:
                # Skip forecasts from more than 1 day ago
                column = "actual"
                precip = np.nan
            else:
                continue

            updated_rows.append({
                "date": forecast_date,
                column: precip
            })

        # Convert to DataFrame
        updates = pd.DataFrame(updated_rows)
        updates["date"] = pd.to_datetime(updates["date"])
        updates['actual'] = (updates['actual'] > 0.1).where(updates['actual'].notna(), np.nan)
        updates = updates.set_index("date")

        # # If no file, create it
        # if not os.path.exists(f"{city}_{FILEPATH}"):
        #     updates.sort_index().to_csv(f"{city}_{FILEPATH}")
        #     continue
        
        # Load and merge
        existing = pd.read_csv(f"{city}_{FILEPATH}", parse_dates=["date"])
        existing = existing.set_index("date")

        combined = existing.combine_first(updates)  # preserve old
        combined.update(updates, overwrite=True)                    # overwrite with new
        combined = combined[["actual", "0_days_out", "1_days_out", "2_days_out", "3_days_out", "4_days_out", "5_days_out", 
                            "6_days_out", "7_days_out", "8_days_out", "9_days_out", "10_days_out", "11_days_out", 
                            "12_days_out", "13_days_out", "14_days_out", "15_days_out"]]
        combined.sort_index().to_csv(f"{city}_{FILEPATH}")

if __name__ == "__main__":
    log_forecast()
    # print(get_daily_data())