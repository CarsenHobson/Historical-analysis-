import os
import pandas as pd


def seconds_to_days_hours(seconds):
    """Convert seconds to days:hours format."""
    days = seconds // (24 * 3600)
    hours = (seconds % (24 * 3600)) // 3600
    return f"{int(days)}:{int(hours)}"


def days_hours_to_seconds(days_hours):
    """Convert days:hours format back to seconds."""
    days, hours = map(int, days_hours.split(':'))
    return (days * 24 + hours) * 3600


def process_csv_file(file_path):
    # Load the CSV file
    data = pd.read_csv(file_path)

    # Convert the 'created_at' column to datetime for easier time manipulation
    data['created_at'] = pd.to_datetime(data['created_at'])

    # Convert 'created_at' to timezone-naive if it contains timezone information
    if data['created_at'].dt.tz is not None:
        data['created_at'] = data['created_at'].dt.tz_convert(None)

    # Filter data for the date range August 13, 2020 – December 2, 2020
    start_date = pd.Timestamp('2020-08-13')
    end_date = pd.Timestamp('2020-12-02')
    data = data[(data['created_at'] >= start_date) & (data['created_at'] <= end_date)]

    # Filter the rows where relay_state is either "ON" or "OFF"
    relay_events = data[['created_at', 'relay_state']].copy()

    # Initialize variables to store results
    on_times = []
    off_times = []
    current_event_start = None

    # Iterate over the relay state to find ON to OFF events
    for i, row in relay_events.iterrows():
        if row['relay_state'] == 'ON' and current_event_start is None:
            # Mark the start of an event
            current_event_start = row['created_at']
        elif row['relay_state'] == 'OFF' and current_event_start is not None:
            # Mark the end of an event and record the event duration
            event_end = row['created_at']
            on_times.append(current_event_start)
            off_times.append(event_end)
            current_event_start = None

    # Calculate the total number of events
    num_events = len(on_times)

    # Calculate the durations of each event in seconds
    event_durations = [(off - on).total_seconds() for on, off in zip(on_times, off_times)]

    # Calculate the time between events (time between each OFF to the next ON) in seconds
    time_between_events = [(on_times[i] - off_times[i - 1]).total_seconds() for i in range(1, len(on_times))]

    # Convert event durations and time between events to days:hours format
    avg_event_duration = seconds_to_days_hours(sum(event_durations) / num_events) if num_events > 0 else "0:0"
    avg_time_between_events = seconds_to_days_hours(sum(time_between_events) / len(time_between_events)) if len(
        time_between_events) > 0 else "0:0"

    return num_events, avg_event_duration, avg_time_between_events


def process_folder(folder_path, output_file_path):
    # Initialize a DataFrame to store results
    all_results = pd.DataFrame()

    # Initialize accumulators for averages
    total_events_sum = 0
    total_event_duration_seconds = 0
    total_time_between_events_seconds = 0
    file_count = 0

    # Cycle through all CSV files in the folder
    for file_name in os.listdir(folder_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(folder_path, file_name)
            print(f"Processing file: {file_name}")

            # Process each CSV file
            num_events, avg_event_duration, avg_time_between_events = process_csv_file(file_path)

            # Append the results for this file into a DataFrame
            all_results[file_name.replace('.csv', '')] = pd.Series({
                "Total Events": num_events,
                "Average Event Duration (days:hours)": avg_event_duration,
                "Average Time Between Events (days:hours)": avg_time_between_events
            })

            # Accumulate totals for averaging later
            total_events_sum += num_events
            total_event_duration_seconds += days_hours_to_seconds(avg_event_duration) * num_events
            total_time_between_events_seconds += days_hours_to_seconds(avg_time_between_events) * (num_events - 1)
            file_count += 1

    # Transpose the DataFrame to get file names as columns
    all_results = all_results.T

    # Calculate overall averages and add a final row with averages
    if file_count > 0:
        avg_total_events = total_events_sum / file_count
        avg_event_duration_overall = seconds_to_days_hours(
            total_event_duration_seconds / total_events_sum) if total_events_sum > 0 else "0:0"
        avg_time_between_events_overall = seconds_to_days_hours(total_time_between_events_seconds / (
                    total_events_sum - file_count)) if total_events_sum - file_count > 0 else "0:0"
        all_results.loc['Averages'] = {
            "Total Events": avg_total_events,
            "Average Event Duration (days:hours)": avg_event_duration_overall,
            "Average Time Between Events (days:hours)": avg_time_between_events_overall
        }

    # Save the DataFrame to a CSV file
    all_results.to_csv(output_file_path, index=True)
    print(f"Results saved to {output_file_path}")


# Specify the folder path where the CSV files are located and the output file path
folder_path = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/Newestalgosim'  # Replace with the path to your CSV folder
output_file_path = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/Currentalgo/EventanalysisCameronPeakfire.csv'  # Replace with the desired output CSV file path

# Run the processing function
process_folder(folder_path, output_file_path)