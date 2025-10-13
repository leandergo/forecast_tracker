import requests
import datetime
import pandas as pd
import numpy as np
import os

# Get the path to the repo root
repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# CSV file to log data
FILEPATH = "nws_forecast_log.csv"

def get_nws_data(lat, lon):
    # Define headers
    headers = {"User-Agent": "YourAppName/1.0 (your.email@example.com)"}

    # Step 1: Get gridpoint
    # lat, lon = 42.3584, -71.0598
    points_url = f"https://api.weather.gov/points/{lat},{lon}"
    response = requests.get(points_url, headers=headers)

    if response.status_code == 200:
        data = response.json()
        forecast_url = data["properties"]["forecast"]

        # Step 2: Get forecast data
        forecast_response = requests.get(forecast_url, headers=headers)
        if forecast_response.status_code == 200:
            forecast_data = forecast_response.json()
            
            # Extract relevant data from periods
            periods = forecast_data["properties"]["periods"]
            data_list = []
            for period in periods:
                if "night" in period.get("name").lower():
                    continue
                # import pdb; pdb.set_trace()
                date = datetime.datetime.strptime(period["startTime"], "%Y-%m-%dT%H:%M:%S%z").date()
                pop = period.get("probabilityOfPrecipitation")["value"]
                data_list.append({
                    "date": date,
                    "probability_of_precipitation": pop
                })
            
            # Create DataFrame
            df = pd.DataFrame(data_list)

            return df
            
        else:
            print(f"Error fetching forecast: {forecast_response.status_code}")
    else:
        print(f"Error fetching points: {response.status_code}")


def nws_log_forecast():
    latitudes = [42.3584, 40.7608, 47.6062]
    longitudes = [-71.0598, -111.8911, -122.3321]
    cities = ["boston", "slc", "seattle"]
    
    for i in range(3):
        df = get_nws_data(latitudes[i], longitudes[i])
        city = cities[i]

        df["date"] = pd.to_datetime(df["date"]).dt.date

        today = datetime.date.today()

        updated_rows = []

        for j, row in df.iterrows():
            forecast_date = row["date"]
            precip = row["probability_of_precipitation"]

            # Determine what kind of column this is
            delta = (forecast_date - today).days


            if delta >= 0:
                column = f"{delta}_days_out"
            else:
                continue

            updated_rows.append({
                "date": forecast_date,
                column: precip
            })

        # Convert to DataFrame
        updates = pd.DataFrame(updated_rows)
        updates["date"] = pd.to_datetime(updates["date"])
        updates['actual'] = np.nan
        updates = updates.set_index("date")

        # # If no file, create it
        # if not os.path.exists(f"{city}_{FILEPATH}"):
        #     updates.sort_index().to_csv(f"{city}_{FILEPATH}")
            
        # else:
        # Build the path to the CSV
        csv_path = os.path.join("nws_data", f"{city}_{FILEPATH}")

        # Load and merge
        existing = pd.read_csv(csv_path, parse_dates=["date"])
        existing = existing.set_index("date")

        combined = existing.combine_first(updates)  # preserve old
        combined.update(updates, overwrite=True)                    # overwrite with new
        combined = combined[["actual", "0_days_out", "1_days_out", "2_days_out", "3_days_out", "4_days_out", "5_days_out", 
                            "6_days_out"]]
        combined.sort_index().to_csv(csv_path)

if __name__ == "__main__":
    nws_log_forecast()