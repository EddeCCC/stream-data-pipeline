import glob
import duckdb as db
from util.logger import logging as log
import os

PARQUET_DIR = "../data/parquet/wikimedia"
WAREHOUSE_PATH = "../data/warehouse/wikimedia.duckdb"
TABLE_NAME = "recent_changes_raw"

def setup_warehouse(connection):
    """Create table for loaded parquet files if non-existing"""
    connection.execute("""
                 CREATE TABLE IF NOT EXISTS loaded_files (
                    file_path VARCHAR PRIMARY KEY,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                 )
                 """)

def list_files():
    """List all parquet files recursively in partition folders"""
    return glob.glob(os.path.join(PARQUET_DIR, "date=*/", "*.parquet"))

def get_new_files(files, connection):
    """Return only files not yet loaded into warehouse file"""
    if not files:
        return []
    loaded = connection.execute("SELECT file_path FROM loaded_files").fetchall()
    loaded_set = {row[0] for row in loaded}
    return [file for file in files if file not in loaded_set]

def load_files(files, connection):
    """Load parquet files into DuckDB and update metadata"""
    table_exists = connection.execute(
        f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{TABLE_NAME}'"
    ).fetchone()[0] > 0

    # Create table if non-existing
    # I believe a central schema would be better, but this should be enough for the demo
    if not table_exists:
        first_file = files[0]
        log.info(f"Creating table {TABLE_NAME} from schema of {first_file}")
        connection.execute(f"CREATE TABLE {TABLE_NAME} AS SELECT * FROM read_parquet('{first_file}') LIMIT 0")

    for file in files:
        log.info(f"Loading data from {file}")
        connection.execute(f"INSERT INTO {TABLE_NAME} SELECT * FROM read_parquet('{file}')")
        connection.execute(f"INSERT INTO loaded_files (file_path) VALUES ('{file}')")

def check_files_count(files, msg):
    """Exit if the there are no files"""
    files_count = len(files)
    if files_count == 0:
        log.info(msg)
        exit(1)

if __name__ == "__main__":
    log.info("Loading data into DuckDB...")

    files = list_files()
    check_files_count(files, f"No parquet files found in {PARQUET_DIR}")

    with db.connect(WAREHOUSE_PATH) as con:
        setup_warehouse(con)

        new_files = get_new_files(files, con)
        check_files_count(new_files, "No new parquet files found")

        load_files(new_files, con)

    log.info("Loading data completed successfully")
