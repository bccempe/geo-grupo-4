import pandas as pd
import json
from datetime import datetime

# Dummy data source
def get_data_from_source():
    """Fetch dummy GTFS data from a source"""
    dummy_data = [
        {"trip_id": "T001", "route_id": "R1", "stop_id": "S1", "arrival_time": "08:00:00", "departure_time": "08:05:00"},
        {"trip_id": "T001", "route_id": "R1", "stop_id": "S2", "arrival_time": "08:15:00", "departure_time": "08:16:00"},
        {"trip_id": "T002", "route_id": "R1", "stop_id": "S1", "arrival_time": "08:30:00", "departure_time": "08:35:00"},
        {"trip_id": "T002", "route_id": "R1", "stop_id": "S3", "arrival_time": "08:45:00", "departure_time": "08:46:00"},
        {"trip_id": "T003", "route_id": "R2", "stop_id": "S2", "arrival_time": "09:00:00", "departure_time": "09:05:00"},
    ]
    return dummy_data

# Normalize data
def normalize_data(raw_data):
    """Normalize raw data into a structured format"""
    df = pd.DataFrame(raw_data)
    
    # Clean and normalize columns
    df['trip_id'] = df['trip_id'].str.strip().str.upper()
    df['route_id'] = df['route_id'].str.strip().str.upper()
    df['stop_id'] = df['stop_id'].str.strip().str.upper()
    
    # Validate time format
    df['arrival_time'] = pd.to_datetime(df['arrival_time'], format='%H:%M:%S', errors='coerce')
    df['departure_time'] = pd.to_datetime(df['departure_time'], format='%H:%M:%S', errors='coerce')
    
    # Remove duplicates
    df = df.drop_duplicates()
    
    return df

# Main execution
if __name__ == "__main__":
    print("=" * 50)
    print("GTFS Data Processing Script")
    print("=" * 50)
    
    # Get raw data
    print("\n1. Fetching data from source...")
    raw_data = get_data_from_source()
    print(f"   Retrieved {len(raw_data)} records")
    
    # Normalize data
    print("\n2. Normalizing data...")
    normalized_df = normalize_data(raw_data)
    print(f"   Normalized to {len(normalized_df)} unique records")
    
    # Display head
    print("\n3. Data Preview (First 5 rows):")
    print("-" * 50)
    print(normalized_df.head())
    
    # Display summary statistics
    print("\n4. Summary Statistics:")
    print("-" * 50)
    print(f"   Total records: {len(normalized_df)}")
    print(f"   Unique trips: {normalized_df['trip_id'].nunique()}")
    print(f"   Unique routes: {normalized_df['route_id'].nunique()}")
    print(f"   Unique stops: {normalized_df['stop_id'].nunique()}")
    
    print("\n" + "=" * 50)
    print("Processing complete!")
    print("=" * 50)
