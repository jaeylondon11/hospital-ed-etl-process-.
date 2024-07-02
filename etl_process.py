import pandas as pd
import os

# Extract
def extract_data(file_path):
    try:
        data = pd.read_csv(file_path)
        print(f"Data extracted successfully with shape {data.shape}")
        return data
    except Exception as e:
        print(f"Error extracting data: {e}")
        return None

# Transform
def transform_data(data):
    # Example transformation: dropping missing values and duplicates
    data = data.dropna().drop_duplicates()
    print(f"Data transformed, new shape: {data.shape}")
    return data

# Load
def load_data(data, output_path):
    try:
        data.to_csv(output_path, index=False)
        print(f"Data loaded to {output_path}")
    except Exception as e:
        print(f"Error loading data: {e}")

# Main ETL function
def etl_process(input_path, output_path):
    data = extract_data(input_path)
    if data is not None:
        transformed_data = transform_data(data)
        load_data(transformed_data, output_path)

if __name__ == "__main__":
    input_file_path = '/mnt/data/hospital-emergency-department-encounters-by-facility.csv'
    output_file_path = '/mnt/data/processed/hospital_emergency_transformed.csv'
    etl_process(input_file_path, output_file_path)
