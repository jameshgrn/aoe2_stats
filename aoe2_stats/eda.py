#%%
# Ensure the directory is correct
import os
import logging
import time

# Setting up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    os.chdir("/Volumes/Gearon's Data Emporium/")
    logging.info("Changed directory to /Volumes/Gearon's Data Emporium/")
except Exception as e:
    logging.error(f"Failed to change directory: {e}")

base_dir = os.getcwd()
import glob
import duckdb
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# Using ThreadPoolExecutor with tqdm for progress tracking
from tqdm import tqdm

# Connect to DuckDB, creating a new database file
try:
    conn = duckdb.connect('aoe2_data.duckdb')
    logging.info("Connected to DuckDB database.")
except Exception as e:
    logging.error(f"Failed to connect to DuckDB: {e}")

try:
    conn.execute("""CREATE OR REPLACE TABLE games AS SELECT * from read_csv('aoe_data.csv')""")
    conn.execute("""CREATE OR REPLACE TABLE buildings AS SELECT * from read_csv('building_masterdata.csv')""")
    conn.execute("""CREATE OR REPLACE TABLE research AS SELECT * from read_csv('research_masterdata.csv')""")
    for i in range(7, 17):
        conn.execute(f"ALTER TABLE research DROP COLUMN column{i:02d}")
    conn.execute("""CREATE OR REPLACE TABLE units AS SELECT * from read_csv('unit_masterdata.csv')""")
    logging.info("Tables created and columns dropped successfully.")
except Exception as e:
    logging.error(f"Error in SQL operations: {e}")

try:
    print(conn.execute("""SHOW TABLES""").fetchall())
except Exception as e:
    logging.error(f"Failed to fetch tables: {e}")

try:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_match_id ON games(match_id)")
    logging.info("Index on match_id created successfully for games table.")
except Exception as e:
    logging.error(f"Failed to create index on games table: {e}")



def process_files_batch(files, table_name, batch_index):
    # Create a new connection for each batch
    conn = duckdb.connect('aoe2_data.duckdb')
    try:
        df_list = [pd.read_csv(file).assign(file=os.path.basename(file)) for file in files]
        df = pd.concat(df_list, ignore_index=True)
        
        if table_name == 'inputs':
            df['match_id'] = df['file'].apply(lambda x: int(x.split('_')[0]))
            df.drop(columns=['file'], inplace=True)  # Remove the 'file' column after extracting match_id
        
        # Drop the first column if it's not needed
        df = df.iloc[:, 1:]  # This drops the first column of the DataFrame

        # Use a unique temporary table name
        temp_table_name = f"temp_{table_name}_{batch_index}_{int(time.time())}"
        conn.register('temp_df', df)
        conn.execute(f"CREATE OR REPLACE TABLE {temp_table_name} AS SELECT * FROM temp_df")
        conn.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table_name}")
        conn.execute(f"DROP TABLE {temp_table_name}")
        logging.info(f"Batch {batch_index} of files inserted into {table_name} table.")
    except Exception as e:
        logging.error(f"Failed to process batch {batch_index} for {table_name}: {e}")
    finally:
        conn.close()  # Close the connection
input_files = glob.glob('/Volumes/Gearon\'s Data Emporium/inputs/inputs/*.csv')
batch_size = 1000  # Adjust batch size based on memory capacity and performance


# Create tables with appropriate schema
try:
    conn.execute("""
        CREATE OR REPLACE TABLE inputs (ts_seconds BIGINT, timestamp VARCHAR, type VARCHAR, param VARCHAR, payload VARCHAR, player VARCHAR, position VARCHAR, x_pos DOUBLE, y_pos DOUBLE, match_id BIGINT)
    """)
    logging.info("Table inputs created with appropriate schema.")
except Exception as e:
    logging.error(f"Failed to create tables with schema: {e}")
# Process files in batches
try:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_files_batch, input_files[i:i+batch_size], 'inputs', i//batch_size) for i in range(0, len(input_files), batch_size)]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Batches"):
            future.result()
    logging.info("All files processed successfully.")
except Exception as e:
    logging.error(f"Error during file processing: {e}")




# Process files in batches
try:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_files_batch, input_files[i:i+batch_size], 'inputs', i//batch_size) for i in range(0, len(input_files), batch_size)]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing Batches"):
            future.result()
    logging.info("All files processed successfully.")
except Exception as e:
    logging.error(f"Error during file processing: {e}")


# Indexing
try:
    conn.execute("CREATE INDEX IF NOT EXISTS idx_match_id ON inputs(match_id)")
    logging.info("Index created successfully on inputs table.")
except Exception as e:
    logging.error(f"Failed to create indexes: {e}")

try:
    print(conn.execute("SHOW TABLES;").fetchall())
except Exception as e:
    logging.error(f"Failed to execute SELECT queries: {e}")

# %%