#%%
# Ensure the directory is correct
import os
import logging
import time

# Setting up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

try:
    os.chdir("/N/scratch/jhgearon/")
    data_basepath = os.getcwd() + "/inputs/inputs"
    logging.info("Changed directory to /N/scratch/jhgearon/")
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

def process_files_batch(files, table_name, batch_index, conn):
    try:
        for file in files:
            file_path = os.path.join(data_basepath, file)
            # Extract match_id from the filename
            match_id = file.split('_')[0]  # Assuming the filename format is "{match_id}_inputs.csv"
            if table_name == 'inputs':
                # Assuming the first column is unnamed and the "position" column is not needed
                # Modify the SQL command to exclude these columns and use the extracted match_id
                conn.execute(f"""
                    COPY {table_name} (
                        ts_seconds, timestamp, type, param, payload, player, x_pos, y_pos
                    ) FROM '{file_path}' (
                        FORMAT 'csv', HEADER, DELIMITER ',', NULL 'None',
                        SKIP 1  # Skip the first column
                    )
                """)
                # Update the match_id directly after the COPY command
                conn.execute(f"UPDATE {table_name} SET match_id = {match_id} WHERE match_id IS NULL;")
        logging.debug(f"Batch {batch_index} of files inserted into {table_name} table.")
        conn.execute("COMMIT;")
        conn.execute("CHECKPOINT;")
    except Exception as e:
        logging.error(f"Failed to process batch {batch_index} for {table_name}: {e}")

input_files = glob.glob('/N/scratch/jhgearon/inputs/inputs/*.csv')
batch_size = 1000  # Increased batch size for better performance

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
        futures = [executor.submit(process_files_batch, input_files[i:i+batch_size], 'inputs', i//batch_size, conn) for i in range(0, len(input_files), batch_size)]
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