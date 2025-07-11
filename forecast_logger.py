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
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 42.3584,
        "longitude": -71.0598,
        "daily": ["precipitation_probability_max", "precipitation_sum"],
        "timezone": "America/New_York",
        "forecast_days": 16,
        "past_days": 1,
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    # print(f"Elevation {response.Elevation()} m asl")
    # print(f"Timezone {response.Timezone()}{response.TimezoneAbbreviation()}")
    # print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process daily data. The order of variables needs to be the same as requested.
    daily = response.Daily()
    daily_precipitation_probability_max = daily.Variables(0).ValuesAsNumpy()
    daily_precipitation_sum = daily.Variables(1).ValuesAsNumpy()

    daily_data = {"date": pd.date_range(
        start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
        end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
        freq = pd.Timedelta(seconds = daily.Interval()),
        inclusive = "left"
    )}

    daily_data["precipitation_probability_max"] = daily_precipitation_probability_max
    daily_data["precipitation_sum"] = daily_precipitation_sum

    daily_dataframe = pd.DataFrame(data = daily_data)

    daily_dataframe['date'] = pd.to_datetime(daily_dataframe['date']).dt.date
    # daily_dataframe['precipitation?'] = (daily_dataframe['precipitation_sum'] > 0)
    
    return daily_dataframe

# Append to CSV
def log_forecast():
    # records = get_daily_data()

    # if os.path.exists(FILENAME):
    #     df_existing = pd.read_csv(FILENAME)
    #     df_all = pd.concat([df_existing, df_new], ignore_index=True)
    # else:
    #     df_all = df_new

    # df_all.to_csv(FILENAME, index=False)
    # print(f"Logged data for {datetime.date.today()}.")
    # Ensure 'date' is date-only
    # Ensure 'date' is date-only
    df = get_daily_data()

    df["date"] = pd.to_datetime(df["date"]).dt.date

    today = datetime.date.today()

    updated_rows = []

    for i, row in df.iterrows():
        forecast_date = row["date"]
        precip = row["precipitation_probability_max"]

        # Determine what kind of column this is
        delta = (forecast_date - today).days

        if delta == -1:
            column = "actual"
        elif delta >= 0:
            column = f"{delta}_days_out"
        else:
            # Skip forecasts from more than 1 day ago
            continue

        updated_rows.append({
            "date": forecast_date,
            column: precip
        })

    # Convert to DataFrame
    updates = pd.DataFrame(updated_rows)
    updates["date"] = pd.to_datetime(updates["date"])
    updates['actual'] = (updates['actual'] > 0).where(updates['actual'].notna(), np.nan)
    updates = updates.set_index("date")

    # If no file, create it
    if not os.path.exists(FILEPATH):
        updates.sort_index().to_csv(FILEPATH)
        return

    # Load and merge
    existing = pd.read_csv(FILEPATH, parse_dates=["date"])
    existing = existing.set_index("date")

    combined = existing.combine_first(updates)  # preserve old
    # combined.update(updates)                    # overwrite with new

    combined.sort_index().to_csv(FILEPATH)

if __name__ == "__main__":
    log_forecast()