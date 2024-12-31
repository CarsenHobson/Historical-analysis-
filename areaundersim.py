import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tqdm import tqdm
from scipy.integrate import trapezoid

AREA_THRESHOLD = 500  # Threshold for turning on the relay
relay_state = 'OFF'  # Initialize the relay state globally
PROCESSED_FOLDER = '/mnt/purpleair/areaunder'
CSV_DIRECTORY = '/mnt/purpleair'


def was_relay_on_between_4am_and_5am(df, date):
    return False  # Placeholder logic


def get_baseline_pm25(baseline_dict, date, previous_baselines):
    return np.mean(previous_baselines) if previous_baselines else 0


def calculate_area_under_curve(data, baseline):
    """Calculate the area under the curve using the trapezoidal rule, considering only values above the baseline."""
    data_above_baseline = np.maximum(np.array(data) - baseline, 0)  # Only values above the baseline are used
    return trapezoid(data_above_baseline)


def process_entire_csv(df, baseline_dict, previous_baselines):
    global relay_state

    print("Converting 'created_at' to datetime...")
    df['created_at'] = pd.to_datetime(df['created_at'], errors='coerce')
    print(f"Total rows after datetime conversion: {len(df)}")

    # Filter rows
    print("Filtering rows for valid months...")
    df = df.loc[df['created_at'].notna() & df['created_at'].dt.month.isin([10, 11, 12, 1, 2, 3])].copy()
    print(f"Total rows after filtering: {len(df)}")

    if df.empty:
        print("No rows to process after filtering.")
        return None

    df.loc[:, 'timestamp'] = pd.to_datetime(df['created_at']).dt.tz_convert('UTC')
    df.loc[:, 'date'] = df['timestamp'].dt.date
    df.loc[:, 'baseline_pm25'] = np.nan
    df.loc[:, 'relay_state'] = 'OFF'  # Default relay state

    cumulative_area = 0  # Tracks the cumulative area under the curve
    baseline_reached = True  # Whether PM2.5 is at or below baseline

    print("Starting row-by-row processing...")
    for i in range(len(df)):
        row = df.iloc[i]
        pm25_value = row['PM2.5_CF1_ug/m3']
        date = row['date']

        # Compute baseline PM2.5 value for the current date
        baseline_pm25 = get_baseline_pm25(baseline_dict, date, previous_baselines)
        df.loc[i, 'baseline_pm25'] = baseline_pm25

        if relay_state == 'OFF':
            # Calculate cumulative area only when relay is OFF
            cumulative_area = calculate_area_under_curve(
                df['PM2.5_CF1_ug/m3'][:i + 1], df['baseline_pm25'][:i + 1]
            )
            if cumulative_area > AREA_THRESHOLD:
                relay_state = 'ON'  # Turn relay ON
                baseline_reached = False  # Relay is ON, so stop resetting area

        elif relay_state == 'ON':
            # Turn relay OFF when PM2.5 drops back to or below the baseline
            if pm25_value <= baseline_pm25:
                relay_state = 'OFF'  # Turn relay OFF
                cumulative_area = 0  # Reset cumulative area
                baseline_reached = True  # Ready to start new cycle

        # Assign the relay state
        df.loc[i, 'relay_state'] = relay_state

    print("Finished processing rows.")
    return df


def process_csv_file(filename, baseline_dict, previous_baselines):
    """Process a single CSV file and return the processed DataFrame."""
    file_path = os.path.join(CSV_DIRECTORY, filename)
    print(f"Reading file: {file_path}")

    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return

    if 'created_at' not in df.columns or 'PM2.5_CF1_ug/m3' not in df.columns:
        print(f"Skipping file {filename}: required columns are missing.")
        return

    print(f"Processing file: {filename}, {len(df)} rows")
    processed_df = process_entire_csv(df, baseline_dict, previous_baselines)

    if processed_df is not None:
        processed_file_path = os.path.join(PROCESSED_FOLDER, f"processed_{filename}")
        processed_df.to_csv(processed_file_path, index=False)
        print(f"Saved processed file: {processed_file_path}")
        plot_data(processed_df, processed_file_path)
    else:
        print(f"No data to process in file: {filename}")


def plot_data(df, file_path):
    """Generate plots for PM2.5 data and relay state."""
    base_filename = os.path.splitext(os.path.basename(file_path))[0]

    # Calculate the percentage of time the relay is "ON"
    relay_on_percentage = df['relay_state'].apply(lambda x: 1 if x == 'ON' else 0).mean() * 100

    # Plot PM2.5 levels with baseline and relay state
    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax1.plot(df['created_at'], df['PM2.5_CF1_ug/m3'], label='PM2.5 Levels', alpha=0.6)
    ax1.plot(df['created_at'], df['baseline_pm25'], label='Baseline PM2.5', linestyle='--')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('PM2.5 (ug/m3)')
    ax1.legend(loc='upper left')
    ax1.grid(True)

    # Highlight relay "ON" points
    relay_on_dates = df[df['relay_state'] == 'ON']['created_at']
    relay_on_values = df[df['relay_state'] == 'ON']['PM2.5_CF1_ug/m3']
    ax1.scatter(relay_on_dates, relay_on_values, color='red', label='Relay State ON', s=10)

    plt.title(f'PM2.5 Levels with Baseline and Relay State\nRelay ON {relay_on_percentage:.2f}% of the time')
    plt.legend(loc='upper right')
    pm25_levels_path = os.path.join(PROCESSED_FOLDER, f'{base_filename}_PM25_levels_with_baseline_and_relay_state.png')
    plt.savefig(pm25_levels_path)
    plt.close()


def cycle_through_csv_files():
    """Cycle through all CSV files in the specified directory."""
    baseline_dict = {}  # Dictionary to store baseline PM2.5 data
    previous_baselines = []  # List to track previous baseline values

    # List CSV files
    csv_files = [f for f in os.listdir(CSV_DIRECTORY) if f.endswith('.csv')]

    if not csv_files:
        print("No CSV files found in the directory.")
        return

    print(f"Found {len(csv_files)} CSV files to process.")
    print("Files:", csv_files)

    # Sequential processing with progress bar
    with tqdm(total=len(csv_files), desc="Processing CSV files", unit="file") as progress_bar:
        for filename in csv_files:
            print(f"Processing file: {filename}")
            try:
                process_csv_file(filename, baseline_dict, previous_baselines)
            except Exception as e:
                print(f"Error processing file {filename}: {e}")
            finally:
                progress_bar.update(1)


def main():
    """Main function to start processing."""
    if not os.path.exists(PROCESSED_FOLDER):
        os.makedirs(PROCESSED_FOLDER)
    cycle_through_csv_files()


if __name__ == "__main__":
    main()