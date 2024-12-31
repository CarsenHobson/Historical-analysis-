import pandas as pd
from datetime import datetime, timedelta
from tqdm import tqdm
import os
import matplotlib.pyplot as plt

# Constants
OUTPUT_FOLDER = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/New files'
PROCESSED_FOLDER = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/Newestalgosim'  # Folder for saving processed CSV files
WINDOW_SIZE = 20  # Number of readings to consider
BASELINE_THRESHOLD_MULTIPLIER = 1.5  # Multiplier to determine if a new baseline is too high

# Create the output folders if they don't exist
os.makedirs(OUTPUT_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

directory = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/New files'

# Global variables
pm25_values = []
current_relay_state = 'OFF'  # Tracks the current relay state

def cycle_through_csv_files():
    print(f"Checking files in directory: {directory}")
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            print(f"Processing file: {file_path}")
            df = pd.read_csv(file_path)
            # Perform the operations on each DataFrame
            process_csv(df, filename)
        else:
            print(f"Skipping non-CSV file: {filename}")

def was_relay_on_between_4am_and_5am(df, date):
    """Check if the relay was ON between 4am and 5am on the given date."""
    start_time = pd.to_datetime(f"{date} 04:00:00").tz_localize('UTC')
    end_time = pd.to_datetime(f"{date} 05:00:00").tz_localize('UTC')
    period = df[(df['created_at'] >= start_time) & (df['created_at'] < end_time)]
    return 'ON' in period['relay_state'].values

def get_baseline_pm25(baseline_dict, current_date, previous_baselines):
    """Determine the appropriate baseline PM2.5 value."""
    current_baseline = baseline_dict.get(current_date, 10)

    # Calculate the average of the previous baselines
    if previous_baselines:
        average_previous_baseline = sum(previous_baselines) / len(previous_baselines)
    else:
        average_previous_baseline = 10

    # Ensure baseline is at least 10
    if current_baseline < 10:
        return 10

    # Check if the new baseline is significantly higher than the average of previous baselines
    if current_baseline > BASELINE_THRESHOLD_MULTIPLIER * average_previous_baseline:
        return 10

    return current_baseline

def process_row(df, index, row, baseline_dict, previous_baselines):
    """Process a single row of PM2.5 data."""
    global current_relay_state

    timestamp = pd.to_datetime(row['created_at'])

    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('UTC')
    else:
        timestamp = timestamp.tz_convert('UTC')

    date = timestamp.date()
    previous_date = (timestamp - timedelta(days=1)).date()

    if was_relay_on_between_4am_and_5am(df, date):
        baseline_pm25 = get_baseline_pm25(baseline_dict, previous_date, previous_baselines)
    else:
        baseline_pm25 = get_baseline_pm25(baseline_dict, date, previous_baselines)

    pm25_value = row['PM2.5_CF1_ug/m3']
    pm25_values.append(pm25_value)

    if len(pm25_values) > WINDOW_SIZE:
        pm25_values.pop(0)

    if len(pm25_values) >= WINDOW_SIZE:
        threshold = 1.25
        # Rising edge logic
        if current_relay_state == 'OFF' and all(data_point > threshold * baseline_pm25 for data_point in pm25_values):
            current_relay_state = 'ON'
        # Falling edge logic
        elif current_relay_state == 'ON' and all(data_point <= baseline_pm25 for data_point in pm25_values):
            current_relay_state = 'OFF'

    # Update previous baselines
    if len(previous_baselines) >= WINDOW_SIZE:
        previous_baselines.pop(0)
    previous_baselines.append(baseline_pm25)

    return baseline_pm25, current_relay_state

def process_csv(df, filename):
    # Load the CSV file
    df['created_at'] = pd.to_datetime(df['created_at'])

    # Extract date and hour from the 'created_at' column
    df['date'] = df['created_at'].dt.date
    df['hour'] = df['created_at'].dt.hour

    # Filter for readings between 5am and 6am
    df_filtered = df[(df['hour'] >= 5) & (df['hour'] < 6)]

    # Calculate the average PM2.5_CF1_ug/m3 for each day
    daily_avg = df_filtered.groupby('date')['PM2.5_CF1_ug/m3'].mean().reset_index()
    daily_avg.rename(columns={'PM2.5_CF1_ug/m3': 'average_PM2_5_CF1_ug_m3'}, inplace=True)

    # Convert the daily_avg to a dictionary for quick lookup
    baseline_dict = dict(zip(daily_avg['date'], daily_avg['average_PM2_5_CF1_ug_m3']))

    # Initialize data storage for PM2.5 values and previous baselines
    global pm25_values
    pm25_values = []
    previous_baselines = []

    # Process each row and add new columns for baseline and relay state
    df['baseline_pm25'] = 0
    df['relay_state'] = 'OFF'

    print("Starting row processing...")
    for index, row in tqdm(df.iterrows(), total=len(df), desc="Processing rows"):
        baseline_pm25, relay_state = process_row(df, index, row, baseline_dict, previous_baselines)
        df.at[index, 'baseline_pm25'] = baseline_pm25
        df.at[index, 'relay_state'] = relay_state

    # Save the updated DataFrame to a new CSV file in the specified processed folder
    output_csv_file_path = os.path.join(PROCESSED_FOLDER, filename.replace('.csv', '_processed.csv'))
    df.to_csv(output_csv_file_path, index=False)
    print(f"Processing completed. Output saved to {output_csv_file_path}")

    # Generate plots
    plot_data(df, output_csv_file_path)

# Plotting function remains unchanged
def plot_data(df, file_path):
    # Same implementation as in your original script
    ...

def main():
    cycle_through_csv_files()

if __name__ == '__main__':
    main()