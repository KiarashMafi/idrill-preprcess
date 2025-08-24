import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
from fastparquet import write

# Parameters for generating exactly 10,000 rows
num_devices = 1  # Only need data from one device
total_rows = 10_000  # Exactly 10,000 rows
start_time = datetime(2025, 1, 1, 0, 0, 0)

failure_types = [
    'Motor_Failure', 'Pump_Leak', 'Hydraulic_Failure',
    'Bit_Wear', 'High_Vibration', 'Sensor_Fault', 'Compressor_Failure'
]


def generate_data():
    timestamps = [start_time + timedelta(seconds=i) for i in range(total_rows)]

    data = {
        'Timestamp': timestamps,
        'Rig_ID': ['RIG_01'] * total_rows,
        'Depth': np.cumsum(np.random.normal(0.002, 0.001, total_rows)),
        'WOB': np.random.normal(1500, 100, total_rows),
        'RPM': np.random.normal(80, 5, total_rows),
        'Torque': np.random.normal(400, 30, total_rows),
        'ROP': np.random.normal(12, 2, total_rows),
        'Mud_Flow_Rate': np.random.normal(1200, 100, total_rows),
        'Mud_Pressure': np.random.normal(3000, 200, total_rows),
        'Mud_Temperature': np.random.normal(60, 3, total_rows),
        'Mud_Density': np.random.normal(1200, 50, total_rows),
        'Mud_Viscosity': np.random.normal(35, 5, total_rows),
        'Mud_PH': np.random.normal(8.5, 0.2, total_rows),
        'Gamma_Ray': np.random.normal(85, 15, total_rows),
        'Resistivity': np.random.normal(20, 5, total_rows),
        'Pump_Status': np.random.choice([0, 1], size=total_rows, p=[0.01, 0.99]),
        'Compressor_Status': np.random.choice([0, 1], size=total_rows, p=[0.02, 0.98]),
        'Power_Consumption': np.random.normal(200, 20, total_rows),
        'Vibration_Level': np.random.normal(0.8, 0.3, total_rows),
        'Bit_Temperature': np.random.normal(90, 5, total_rows),
        'Motor_Temperature': np.random.normal(75, 4, total_rows),
    }

    # Add failure flags and types
    failure_flags = np.zeros(total_rows, dtype=int)
    failure_count = int(0.05 * total_rows)  # 5% failure rate
    failure_indices = random.sample(range(total_rows), failure_count)
    for idx in failure_indices:
        failure_flags[idx] = 1
    data['Maintenance_Flag'] = failure_flags

    failure_type_col = ['None'] * total_rows
    for idx in failure_indices:
        failure_type_col[idx] = random.choice(failure_types)
    data['Failure_Type'] = failure_type_col

    df = pd.DataFrame(data)

    # Add missing values (5% of cells)
    n_cells = df.size
    n_missing = int(n_cells * 0.05)
    for _ in range(n_missing):
        i = random.randint(0, df.shape[0] - 1)
        j = random.randint(0, df.shape[1] - 1)
        if df.columns[j] in ['Timestamp', 'Rig_ID', 'Failure_Type', 'Maintenance_Flag']:
            continue
        df.iat[i, j] = np.nan

    # Add noise (3% of cells)
    n_noisy = int(n_cells * 0.03)
    float_cols = df.select_dtypes(include=['float64']).columns
    for _ in range(n_noisy):
        i = random.randint(0, df.shape[0] - 1)
        j = random.choice(float_cols)
        original = df.at[i, j]
        if pd.isna(original):
            continue
        noise = original * 0.1 * (np.random.rand() * 2 - 1)
        df.at[i, j] = original + noise

    return df


def save_data(df):
    # Create output directory if it doesn't exist
    output_dir = "output_data"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # Save as CSV
    csv_path = os.path.join(output_dir, "rig_data_10000.csv")
    df.to_csv(csv_path, index=False)

    # Save as Parquet
    parquet_path = os.path.join(output_dir, "rig_data_10000.parquet")
    write(parquet_path, df, compression='snappy')

    print(f"Data saved to:\n{csv_path}\n{parquet_path}")


if __name__ == "__main__":
    # Generate the data
    df = generate_data()

    # Verify we have exactly 10,000 rows
    print(f"Generated {len(df)} rows of data")

    # Save the data
    save_data(df)

    print("Data generation completed.")