import pandas as pd
import sqlite3

# Load the CSV file
file_path = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/New files/Bucking House (outside) (40.555024 -105.035172) Primary Real Time 1_1_2015 7_1_2022.csv'
df = pd.read_csv(file_path)

# Parse the 'created_at' column as datetime
df['created_at'] = pd.to_datetime(df['created_at'])

# Extract date and hour from the 'created_at' column
df['date'] = df['created_at'].dt.date
df['hour'] = df['created_at'].dt.hour

# Filter for readings between 5am and 6am
df_filtered = df[(df['hour'] >= 5) & (df['hour'] < 6)]

# Calculate the average PM2.5_CF1_ug/m3 for each day
daily_avg = df_filtered.groupby('date')['PM2.5_CF1_ug/m3'].mean().reset_index()
daily_avg.rename(columns={'PM2.5_CF1_ug/m3': 'average_PM2_5_CF1_ug_m3'}, inplace=True)

# Connect to SQLite database (or create it)
conn = sqlite3.connect('Bucking housenew2.db')
cursor = conn.cursor()

# Drop the existing table if it exists
cursor.execute('DROP TABLE IF EXISTS daily_averages')

# Create a new table for storing daily averages
cursor.execute('''
CREATE TABLE daily_averages (
    date TEXT PRIMARY KEY,
    average_PM2_5_CF1_ug_m3 REAL
)
''')

# Insert data into the table
for index, row in daily_avg.iterrows():
    cursor.execute('''
    INSERT OR REPLACE INTO daily_averages (date, average_PM2_5_CF1_ug_m3)
    VALUES (?, ?)
    ''', (row['date'], row['average_PM2_5_CF1_ug_m3']))

# Commit the changes and close the connection
conn.commit()
conn.close()