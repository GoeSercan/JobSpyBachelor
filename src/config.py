import os
import json
from dotenv import load_dotenv

load_dotenv()

# Load proxy data
BRD_USER = os.getenv("BRD_USER")
BRD_ZONE = os.getenv("BRD_ZONE")
BRD_PASSWD = os.getenv("BRD_PASSWD")
BRD_SUPERPROXY = os.getenv("BRD_SUPERPROXY")
CA_CERT_PATH = os.getenv("CA_CERT_PATH")

#Load db data
DB_NAME=os.getenv("DB_NAME")
DB_USER=os.getenv("DB_USER")
DB_PASSW=os.getenv("DB_PASSW")
DB_HOST=os.getenv("DB_HOST")

# File paths
OFFSET_FILE = "offsets.json"
DAILY_JOB_COUNT_FILE = "daily_job_counts.json"
LOG_FILE = "scraping_log.txt"

# Parameters for scraper
RESULTS_PER_PAGE_OPTIONS = [40]
NO_RESULT_THRESHOLD = 2

# Parameters for runner
MAX_RUNS = 576
INTERVAL_SECONDS = 150

# Load JSON files
with open("countries.json") as f:
    COUNTRIES = json.load(f)

with open("search_terms.json") as f:
    SEARCH_TERMS = json.load(f)["search_terms"]
