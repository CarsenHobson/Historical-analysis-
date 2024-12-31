import os
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Define the folder containing the CSV files
folder_path = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/Mixing files'

# Specify the time frame for processing
start_time = '2019-01-01 00:00:00'
end_time = '2023-12-31 23:59:59'

# Threshold for elevated indoor PM2.5 levels
elevated_threshold = 50  # µg/m³

# Output file for storing results
output_csv = os.path.join(folder_path, "relay_elevated_percentages_by_file.csv")

# Function to check if a file is empty
def is_file_empty(file_path):
    try:
        return os.stat(file_path).st_size == 0
    except OSError:
        return True

# Function to process and save data
def process_and_save(file_path, start_time, end_time, results):
    print(f"Processing file: {file_path}")

    # Check if the file is empty
    if is_file_empty(file_path):
        print(f"Skipping file {file_path} because it is empty.")
        return

    # Load the data
    try:
        data = pd.read_csv(file_path)
    except pd.errors.EmptyDataError:
        print(f"Skipping file {file_path} because it contains no data.")
        return

    # Check if required columns exist
    required_columns = ['created_at', 'PM2.5_CF1_ug/m3', 'Estimated_Indoor_PM2.5', 'relay_state']
    missing_columns = [col for col in required_columns if col not in data.columns]

    if missing_columns:
        print(f"Skipping file {file_path} due to missing columns: {', '.join(missing_columns)}")
        return

    # Clean the data
    data['created_at'] = pd.to_datetime(data['created_at'], errors='coerce')
    data = data.dropna(subset=['created_at', 'PM2.5_CF1_ug/m3', 'Estimated_Indoor_PM2.5', 'relay_state'])

    # Filter data based on the specified time frame
    data = data[(data['created_at'] >= start_time) & (data['created_at'] <= end_time)]

    if data.empty:
        print(f"No data within the specified time frame for file: {file_path}")
        return

    # Add a column to indicate if indoor PM2.5 is elevated
    data['elevated'] = data['Estimated_Indoor_PM2.5'] > elevated_threshold

    # Separate data where the relay is ON
    relay_on_data = data[data['relay_state'] == "ON"]

    # Calculate metrics
    elevated_when_relay_on = relay_on_data['elevated'].sum()
    total_elevated = data['elevated'].sum()
    percentage_elevated_when_relay_on = (elevated_when_relay_on / total_elevated * 100) if total_elevated > 0 else 0

    # Add the result to the results list
    results.append({
        'File': os.path.basename(file_path),
        'Percentage_Elevated_When_Relay_ON': percentage_elevated_when_relay_on
    })

    # Print metrics
    print(f"File: {os.path.basename(file_path)} - Percentage of elevated indoor PM2.5 when relay ON: {percentage_elevated_when_relay_on:.2f}%")

    # Time Series Plot
    plt.figure(figsize=(12, 6))
    #plt.plot(data['created_at'], data['Estimated_Indoor_PM2.5'], label='Estimated Indoor PM2.5', color='green')
    plt.plot(data['created_at'], data ['PM2.5_CF1_ug/m3'], label='Outdoor PM2.5', color='blue')
    plt.axhline(y=elevated_threshold, color='orange', linestyle='--', label='Elevated Threshold')
    #plt.scatter(relay_on_data['created_at'], relay_on_data['Estimated_Indoor_PM2.5'],
                #color='red', label='Relay ON', zorder=5)

    plt.xlabel('Time')
    plt.ylabel('PM2.5 Concentration (µg/m³)')
    plt.title(f'Indoor PM2.5 Levels and Relay State\n{os.path.basename(file_path)}')
    plt.legend()
    plt.grid()
    plt.tight_layout()

    # Save the plot
    output_dir = os.path.join(folder_path, "plots2")
    os.makedirs(output_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(file_path))[0]
    plot_path = os.path.join(output_dir, f"{base_name}_indoor_relay.png")
    plt.savefig(plot_path)
    plt.close()

    print(f"Plot saved to: {plot_path}")


# List to store results
results = []

# Iterate through all CSV files in the folder
for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv'):
        file_path = os.path.join(folder_path, file_name)
        process_and_save(file_path, start_time, end_time, results)

# Save all results to the output CSV
results_df = pd.DataFrame(results)
results_df.to_csv(output_csv, index=False)

# Notify user of output CSV location
print(f"Percentages by file saved to: {output_csv}")