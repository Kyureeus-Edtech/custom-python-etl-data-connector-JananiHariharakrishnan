from dotenv import load_dotenv
import os
import requests
from pymongo import MongoClient
import datetime

# =======================================================
# 1. Load Environment Variables
# =======================================================
load_dotenv()

RAWG_API_KEY = os.getenv("RAWG_API_KEY")       # API key for RAWG
MONGO_URI = os.getenv("MONGO_URI")             # MongoDB connection string
DB_NAME = os.getenv("DB_NAME", "SSN_ETL_assignment")        # Database name configurable via .env
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "rawg_platforms_raw")  # Better collection name

BASE_URL = "https://api.rawg.io/api/platforms"  # RAWG API endpoint for platforms

# =======================================================
# 2. Connect to MongoDB
# =======================================================
mongo_client = MongoClient(MONGO_URI)
database = mongo_client[DB_NAME]
collection = database[COLLECTION_NAME]

# =======================================================
# 3. Extract Function
# =======================================================
def fetch_platforms(page_number=1, page_size=20):
    
    params = {
        "key": RAWG_API_KEY,
        "page": page_number,
        "page_size": page_size
    }
    
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()  # Raises HTTPError for bad responses
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        if response.status_code == 429:
            print("Rate limit reached. Please try again later.")
        else:
            print(f"HTTP error occurred: {http_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        print(f"Network or connection error: {req_err}")
        raise

# =======================================================
# 4. Transform Function
# =======================================================
def transform_platforms(api_response):

    transformed_platforms = []
    
    for platform in api_response.get("results", []):
        platform_record = {
            "id": platform.get("id"),
            "name": platform.get("name"),
            "slug": platform.get("slug"),
            "games_count": platform.get("games_count"),
            "image_background": platform.get("image_background"),
            "year_start": platform.get("year_start"),
            "year_end": platform.get("year_end"),
            "ingestion_timestamp": datetime.datetime.utcnow(),
            "games": [
                {
                    "id": game.get("id"),
                    "slug": game.get("slug"),
                    "name": game.get("name"),
                    "added": game.get("added")
                }
                for game in platform.get("games", [])
            ]
        }
        transformed_platforms.append(platform_record)
    
    return transformed_platforms

# =======================================================
# 5. Load Function
# =======================================================
def load_to_mongodb(platform_records):

    for record in platform_records:
        try:
            collection.update_one(
                {"id": record["id"]},
                {"$set": record},
                upsert=True
            )
        except Exception as e:
            print(f"Error inserting/updating platform ID {record['id']}: {e}")

# =======================================================
# 6. Main ETL Process
# =======================================================
def main():

    page_number = 1
    
    while True:
        print(f"Fetching page {page_number} from RAWG API...")
        
        try:
            api_response = fetch_platforms(page_number=page_number)
        except Exception:
            print("Stopping ETL due to error during extraction.")
            break
        
        if not api_response.get("results"):
            print("No more data found. Ending ETL process.")
            break
        
        transformed_records = transform_platforms(api_response)
        
        if transformed_records:
            load_to_mongodb(transformed_records)
        else:
            print("No valid records to insert for this page, skipping database operation.")
        
        if api_response.get("next"):
            page_number += 1
        else:
            break
    
    print("ETL process completed successfully!")

# =======================================================
# Script Entry Point
# =======================================================
if __name__ == "__main__":
    main()
