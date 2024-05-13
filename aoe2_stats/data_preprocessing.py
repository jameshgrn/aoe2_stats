import pandas as pd
import os
from concurrent.futures import ProcessPoolExecutor
import glob

def preprocess_csv(file_path):
    try:
        # Load the CSV file, assuming no header for the first row and skipping it
        df = pd.read_csv(file_path, header=0)
        
        # Drop the first column and the 'position' column
        df.drop(df.columns[[0, 7]], axis=1, inplace=True)  # Adjust the index 6 if the position of 'position' column changes
        
        # Save the modified DataFrame back to the file
        df.to_csv(file_path, index=False)
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

def main():
    # Directory containing the CSV files
    directory_path = '/N/scratch/jhgearon/inputs/inputs/'
    
    # List all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory_path, '*.csv'))
    
    # Process files in parallel
    with ProcessPoolExecutor() as executor:
        executor.map(preprocess_csv, csv_files)

if __name__ == "__main__":
    main()