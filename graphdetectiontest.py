import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Connect to the SQLite database
db_path = '/Users/carsenhobson/Downloadsw/detectiontest.db'
conn = sqlite3.connect(db_path)

# Load data from the relevant tables
detectiontest_query = "SELECT * FROM detectiontestV2;"
baseline_query = "SELECT * FROM BaselineData;"
baseline_value_query = "SELECT * FROM BaselineValue;"

detectiontest_df = pd.read_sql(detectiontest_query, conn)
baseline_df = pd.read_sql(baseline_query, conn)
baseline_value_df = pd.read_sql(baseline_value_query, conn)

# Close the connection
conn.close()

# Convert timestamp to datetime
detectiontest_df['timestamp'] = pd.to_datetime(detectiontest_df['timestamp'], unit='s')
baseline_value_df['timestamp'] = pd.to_datetime(baseline_value_df['timestamp'], unit='s')

# Convert relaystate to boolean
detectiontest_df['relay_on'] = detectiontest_df['relaystate'].apply(lambda x: 1 if x == 'ON' else 0)

# Drop rows with NaN values in the 'pm25' column
cleaned_detectiontest_df = detectiontest_df.dropna(subset=['pm25'])

# Ensure the cleaned dataframe has the correct types
cleaned_detectiontest_df['pm25'] = cleaned_detectiontest_df['pm25'].astype(float)

# Set the date range for the x-axis
start_date = pd.to_datetime("2024-07-15")
end_date = pd.to_datetime(datetime.now())

# Filter data within the specified date range
cleaned_detectiontest_df = cleaned_detectiontest_df[(cleaned_detectiontest_df['timestamp'] >= start_date) &
                                                    (cleaned_detectiontest_df['timestamp'] <= end_date)]
baseline_value_df = baseline_value_df[(baseline_value_df['timestamp'] >= start_date) &
                                      (baseline_value_df['timestamp'] <= end_date)]

# Plot PM2.5 levels and Baseline PM2.5 levels without using fill_between
plt.figure(figsize=(14, 7))

# Plot PM2.5 levels
plt.plot(cleaned_detectiontest_df['timestamp'], cleaned_detectiontest_df['pm25'], label='PM2.5 Levels', color='blue')

# Plot Baseline PM2.5 levels
plt.plot(baseline_value_df['timestamp'], baseline_value_df['baseline_pm2_5'], label='Baseline PM2.5 Levels', color='green')

# Plot relay state as dots
relay_on = cleaned_detectiontest_df[cleaned_detectiontest_df['relay_on'] == 1]
plt.scatter(relay_on['timestamp'], relay_on['pm25'], color='red', label='Relay ON', alpha=0.5)

# Labels and title
plt.xlabel('Timestamp')
plt.ylabel('PM2.5 Levels')
plt.title('PM2.5 Levels, Baseline Levels, and Relay Status')
plt.legend()
plt.grid(True)

# Set x-axis limits
plt.xlim(start_date, end_date)

# Show plot
plt.show()