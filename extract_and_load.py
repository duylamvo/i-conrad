import os
import gdown
import zipfile
import sqlite3
import shutil
import pandas as pd

GOOGLE_DRIVE_FILE = (
    "https://drive.google.com/file/d/1vcb_HBWsOSKW4XxhLfRpGlLzBLwHlGWJ/view?ts=642d8042"
)
DEFAULT_DATA_DIR = "data"
DEFAULT_DBT_DIR = "dbt"
DEFAULT_DATABASE_FILE = f"{DEFAULT_DBT_DIR}/data/thelook_ecommerce.db"


def get_download_link(file_path: str) -> str:
    """Extract file IDs and get download link from drive path."""
    file_id = None
    try:
        file_id = file_path.split("/d/")[1].split("/")[0]

        url = f"https://drive.google.com/uc?id={file_id}"
        return url

    except IndexError:
        raise ValueError("Invalid google link format.")


def download(file_path: str):
    """Download and extract file path."""

    url = get_download_link(file_path)
    output_file_path = gdown.download(url=url)

    print(f"File downloaded at {output_file_path}")
    return output_file_path


def extract_zip_files(zip_file: str, to_dir: str = DEFAULT_DATA_DIR):
    """Extract zip files to a folder."""
    with zipfile.ZipFile(zip_file, "r") as zip_ref:
        # Extract all the contents to the specified directory
        zip_ref.extractall(to_dir)

        print(f"All files extracted to {to_dir}")


def load_to_db(
    conn: sqlite3.Connection,
    data_dir: str = DEFAULT_DATA_DIR,
    if_exists: str = "replace",
):
    """Load all csv files to sqlite db."""
    for file_name in os.listdir(data_dir):
        if file_name.endswith(".csv"):
            file_path = os.path.join(data_dir, file_name)

            df = pd.read_csv(file_path)

            # Get table name from file name
            table_name = os.path.splitext(file_name)[0]

            # Load to DB for later query
            df.to_sql(name=table_name, con=conn, if_exists=if_exists, index=False)

            print(f"Loaded {file_name} into table {table_name}")

    print("All files has been processed")


def clean():
    """Clean."""
    shutil.rmtree(DEFAULT_DATA_DIR)
    files = ["thelook_ecommerce.zip", DEFAULT_DATABASE_FILE]
    for f in files:
        if os.path.isfile(f):
            os.remove(f)


def ingest_gdrive(
    gdrive_link: str = GOOGLE_DRIVE_FILE,
    extract_to_dir: str = DEFAULT_DATA_DIR,
    db_file=DEFAULT_DATABASE_FILE,
    if_exists="replace",
):
    """From google drive data files to sqlite."""
    # Download and extract gdrive to folder
    output_file_path = download(gdrive_link)

    if output_file_path.endswith(".zip"):
        extract_zip_files(output_file_path, to_dir=extract_to_dir)

    conn = sqlite3.connect(db_file)

    load_to_db(conn=conn, data_dir=extract_to_dir, if_exists=if_exists)

    conn.close()


def main():
    db_file = DEFAULT_DATABASE_FILE
    conn = sqlite3.connect(db_file)

    # List tables
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn)
    tables = tables.name.to_list()
    # ['products', 'orders', 'inventory_items', 'users', 'distribution_centers', 'events', 'order_items']
    for t in tables:
        print(f"-----{t}-----")
        t = "users"
        df = pd.read_csv(f"data/{t}.csv")
        print(df.head(10))
        df.columns
