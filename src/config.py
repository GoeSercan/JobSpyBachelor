import os
import json
from dotenv import load_dotenv

load_dotenv()

# Load sensitive data
BRD_USER = os.getenv("BRD_USER")
BRD_ZONE = os.getenv("BRD_ZONE")
BRD_PASSWD = os.getenv("BRD_PASSWD")
BRD_SUPERPROXY = os.getenv("BRD_SUPERPROXY")
CA_CERT_PATH = os.getenv("CA_CERT_PATH")

# Load JSON files
with open("countries.json") as f:
    COUNTRIES = json.load(f)

with open("search_terms.json") as f:
    SEARCH_TERMS = json.load(f)["search_terms"]
