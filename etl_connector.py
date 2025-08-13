from dotenv import load_dotenv
import os
import requests
from pymongo import MongoClient
import datetime

# =======================================================
# 1. Load Environment Variables
# =======================================================
load_dotenv()

OTX_API_KEY = os.getenv("OTX_API_KEY", "").strip()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

# New optional params from .env
LIMIT = int(os.getenv("LIMIT"))  # default 10 results per page
MODIFIED_SINCE = os.getenv("MODIFIED_SINCE")  # ISO 8601 datetime string or None

BASE_URL = "https://otx.alienvault.com/api/v1/pulses/subscribed"

# =======================================================
# 2. Connect to MongoDB
# =======================================================
mongo_client = MongoClient(MONGO_URI)
database = mongo_client[DB_NAME]
collection = database[COLLECTION_NAME]

# =======================================================
# 3. Extract Function
# =======================================================
def fetch_pulses(page_number=1, limit=LIMIT, modified_since=MODIFIED_SINCE):
    headers = {
        "X-OTX-API-KEY": OTX_API_KEY
    }
    
    params = {
        "limit": limit,
        "page": page_number
    }
    
    if modified_since:
        params["modified_since"] = modified_since
    
    try:
        response = requests.get(BASE_URL, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.text}")
        return None
    except requests.exceptions.RequestException as req_err:
        print(f"Network or connection error: {req_err}")
        return None

# =======================================================
# 4. Transform Function
# =======================================================
def transform_pulses(api_response):
    transformed_pulses = []
    
    for pulse in api_response.get("results", []):
        pulse_record = {
            "id": pulse.get("id"),
            "name": pulse.get("name"),
            "author_name": pulse.get("author_name"),
            "description": pulse.get("description"),
            "created": pulse.get("created"),
            "modified": pulse.get("modified"),
            "tags": pulse.get("tags", []),
            "references": pulse.get("references", []),
            "targeted_countries": pulse.get("targeted_countries", []),
            "indicators": [
                {
                    "indicator": i.get("indicator"),
                    "type": i.get("type"),
                    "title": i.get("title"),
                    "description": i.get("description")
                }
                for i in pulse.get("indicators", [])
            ],
            "ingestion_timestamp": datetime.datetime.utcnow()
        }
        transformed_pulses.append(pulse_record)
    
    return transformed_pulses

# =======================================================
# 5. Load Function
# =======================================================
def load_to_mongodb(pulse_records):
    for record in pulse_records:
        try:
            collection.update_one(
                {"id": record["id"]},
                {"$set": record},
                upsert=True
            )
        except Exception as e:
            print(f"Error inserting/updating pulse ID {record['id']}: {e}")

# =======================================================
# 6. Main ETL Process
# =======================================================
def main():
    page_number = 1
    success = True
    
    while True:
        print(f"Fetching page {page_number} from OTX Pulses API...")
        
        api_response = fetch_pulses(page_number=page_number)
        if not api_response:
            print("Stopping ETL due to error during extraction.")
            success = False
            break
        
        if not api_response.get("results"):
            print("No more data found. Ending ETL process.")
            break
        
        transformed_records = transform_pulses(api_response)
        
        if transformed_records:
            load_to_mongodb(transformed_records)
        else:
            print("No valid records to insert for this page, skipping database operation.")
        
        if api_response.get("next"):
            page_number += 1
        else:
            break
    
    if success:
        print("ETL process completed successfully!")
    else:
        print("ETL process ended with errors.")

# =======================================================
# Script Entry Point
# =======================================================
if __name__ == "__main__":
    main()
