import pandas as pd
import numpy as np
from scipy.integrate import solve_ivp
import os

# Define the directory containing the CSV files
input_directory = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/Newestalgosim'  # Replace with your directory path
output_directory = '/Users/carsenhobson/Downloads/sapphires_potential_cities/fort_collins/Mixing files'  # Replace with your desired output directory

# Ensure the output directory exists
os.makedirs(output_directory, exist_ok=True)

# Room-specific parameters
V = 100    # Room volume (m³)
Q = 1    # Airflow rate (m³/h, corresponding to 1.0 ACH)
k = 0.05  # Removal rate constant (no HEPA filtration)

# Differential equation model
def model(t, C, V, Q, k, pm_concentration_in):
    C_in = np.interp(t, time_points, pm_in)  # Interpolate outdoor concentration
    dCdt = (Q/V) * (C_in - C) - k * C        # Calculate rate of change
    return dCdt

# Cycle through all CSV files in the directory
for file_name in os.listdir(input_directory):
    if file_name.endswith('.csv'):
        file_path = os.path.join(input_directory, file_name)
        print(f"Processing: {file_name}")

        # Load the CSV file
        data = pd.read_csv(file_path)

        # Ensure 'created_at' is parsed and 't_numeric' is created
        data['created_at'] = pd.to_datetime(data['created_at'])
        data['t_numeric'] = (data['created_at'] - data['created_at'].iloc[0]).dt.total_seconds() / 3600

        # Extract numeric time points and PM2.5 concentration
        time_points = data['t_numeric'].values
        pm_in = data['PM2.5_CF1_ug/m3'].values

        # Solve the differential equation using solve_ivp
        time_sim = np.linspace(time_points[0], time_points[-1], 2000)
        solution = solve_ivp(
            model,
            [time_points[0], time_points[-1]],
            [0],  # Initial condition C0 = 0
            t_eval=time_sim,
            args=(V, Q, k, pm_in),
            method='RK45'  # Runge-Kutta solver
        )

        # Extract the simulated concentrations
        C_sim = solution.y[0]

        # Interpolate simulated indoor values for the original timestamps
        data['Estimated_Indoor_PM2.5'] = np.maximum(0, np.interp(data['t_numeric'], time_sim, C_sim))

        # Save the updated data with indoor estimates
        output_path = os.path.join(output_directory, f"Updated_{file_name}")
        data.to_csv(output_path, index=False)
        print(f"Saved: {output_path}")