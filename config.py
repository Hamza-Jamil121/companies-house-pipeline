    # config.py
import os
import re
from pathlib import Path
import psutil

from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT")),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}

DOWNLOAD_URL =   os.getenv("DOWNLOAD_URL") 


match = re.search(r"Accounts_Monthly_Data-(\w+)(\d{4})\.zip", DOWNLOAD_URL)
if not match:
    raise ValueError("Cannot extract month and year from DOWNLOAD_URL")

MONTH_NAME, YEAR = match.groups()  # e.g., "April", "2025"
MONTH_YEAR = f"{MONTH_NAME}{YEAR}"  # "April2025"
PIPELINE_MONTH = f"{MONTH_NAME[:3]}-{YEAR[-2:]}"  # e.g., "Apr-25"

# config.py

DATA_DIR = os.getenv("DATA_DIR") 
EXTRACT_DIR = rf"{DATA_DIR}\extracted"
ZIP_PATH = rf"{DATA_DIR}\{MONTH_NAME[:3]}_{YEAR[-4:]}.zip"


NUM_WORKERS = int(os.getenv("NUM_WORKERS"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE") )



# -----------------------------
# Local directories
# -----------------------------
name=f"Accounts_Monthly_Data-{MONTH_YEAR}.zip"

# python -c "import multiprocessing; print(f'CPU cores: {multiprocessing.cpu_count()}')"