import pandas as pd
import zipfile
import os

# Define the input zip file and output directory
zip_file_path = 'gtfs_subway.zip'
output_dir = 'cleaned_gtfs_data'

# Create output directory if it doesn't exist
if not os.path.exists(output_dir):
    os.makedirs(output_dir)

def clean_gtfs_data(zip_path, output_path):
    print(f"Processing {zip_path}...")
    
    try:
        with zipfile.ZipFile(zip_path, 'r') as z:
            # List all text files in the zip
            files = [f for f in z.namelist() if f.endswith('.txt')]
            
            for file_name in files:
                print(f"\n--- Processing {file_name} ---")
                
                # Read the CSV file directly from the zip
                # GTFS files are CSVs despite the .txt extension
                try:
                    df = pd.read_csv(z.open(file_name))
                except pd.errors.EmptyDataError:
                    print(f"Skipping {file_name}: File is empty.")
                    continue

                # 1. Identify columns with > 10% missing values
                missing_percent = df.isnull().mean() * 100
                high_missing_cols = missing_percent[missing_percent > 10]
                
                if not high_missing_cols.empty:
                    print("Columns with > 10% missing values:")
                    for col, pct in high_missing_cols.items():
                        print(f"  - {col}: {pct:.2f}%")
                else:
                    print("No columns found with > 10% missing values.")

                # 2. Standardize Date Columns to YYYY-MM-DD
                # Common GTFS date columns: 'date', 'start_date', 'end_date'
                date_cols = [col for col in df.columns if 'date' in col.lower()]
                
                if date_cols:
                    print(f"Standardizing date columns: {date_cols}")
                    for col in date_cols:
                        # GTFS dates are typically strings like 'YYYYMMDD'
                        # We convert them to datetime objects, then to 'YYYY-MM-DD' strings
                        try:
                            # Coerce errors to NaT to handle invalid formats gracefully
                            df[col] = pd.to_datetime(df[col], format='%Y%m%d', errors='coerce')
                            df[col] = df[col].dt.strftime('%Y-%m-%d')
                        except Exception as e:
                            print(f"  Warning: Could not standardize column '{col}': {e}")
                
                # Save the cleaned dataframe to a new CSV
                output_file = os.path.join(output_path, file_name.replace('.txt', '_cleaned.csv'))
                # Write to CSV, ensuring no index is saved
                df.to_csv(output_file, index=False)
                print(f"Saved cleaned file to: {output_file}")

    except FileNotFoundError:
        print(f"Error: The file {zip_path} was not found.")
    except zipfile.BadZipFile:
        print(f"Error: The file {zip_path} is not a valid zip file.")

# Run the cleaning function
if __name__ == "__main__":
    # Ensure the zip file is in the same directory or provide the full path
    clean_gtfs_data(zip_file_path, output_dir)