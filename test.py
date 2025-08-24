import pandas as pd

# Load your CSV
df = pd.read_csv("output_data/rig_data_10000.csv")

# Compute mean of all numeric columns
column_means = df.mean(numeric_only=True).to_dict()

# Print the results
for col, mean_value in column_means.items():
    print(f"{col}: {mean_value}")
