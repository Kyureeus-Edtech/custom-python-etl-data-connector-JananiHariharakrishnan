
## RAWG Platforms ETL Pipeline (Python to MongoDB) 
---
**Project Overview**  
This project implements an ETL (Extract – Transform – Load) process that takes video game platform data from the RAWG.io API, processes it to keep only essential details, and stores it in a MongoDB collection.  

The script does the following:  
- Extracts platform data from the RAWG API using API key authentication and pagination
  - How
- Transforms the data to select only relevant fields for analysis  
- Loads the cleaned data into MongoDB using upsert so it can be updated without duplicates  
- Adds an ingestion timestamp to each record for auditing  
---
**API Endpoint Details**  
Base URL: https://api.rawg.io/api/platforms  
You must supply your API key using the parameter `key=YOUR_API_KEY`.  

Example endpoint:  
```
https://api.rawg.io/api/platforms?key=YOUR_API_KEY&page=1&page_size=20
```
---
**Common parameters**:  
- page: for pagination  
- page_size: number of records returned per page (default is 20)  
- key: your API key  

In this ETL we keep:  
- **id, name, slug** – uniquely identify each platform and provide both a human-readable name and a URL-friendly identifier  
- **games_count, image_background** – total games for the platform and a visual reference for UI or analysis  
- **year_start, year_end** – capture the lifespan of the platform, useful for historical analysis  
- **games list (id, slug, name, added)** – top associated games in a compact format (only key metadata kept for speed and clarity)  
- **ingestion_timestamp** – added by the script to record when the data was fetched, supports auditing and freshness checks  
---
**Setup Instructions** 
1. Clone the repository and open the project folder.  

2. Create a `.env` file with the following content:  
```
RAWG_API_KEY=your_api_key_here
MONGO_URI=mongodb://localhost:27017/
DB_NAME=SSN_ETL_assignment
COLLECTION_NAME=rawg_platforms_processed
```

3. Install dependencies:  
```
pip install -r requirements.txt
```

4. Make sure MongoDB is running locally or connect to a remote instance.  

5. Run the ETL script:  
```
python etl_connector.py
```
---
**Testing and Validation**  
The script includes several checks to ensure quality:  
- Uses `raise_for_status()` to stop on HTTP errors such as 400, 401, 429 or 500  
- Detects and warns about rate limits (HTTP 429)  
- Skips inserting data if the API response is empty  
- Uses upsert in MongoDB to either insert new data or update existing records without duplicates  
- Captures and logs network or database errors  
- Adds ingestion_timestamp to every record for traceability  
---
**Why the Code is Self‑Documenting**  
- Clear, descriptive function names such as fetch_platforms, transform_platforms, load_to_mongodb  
- Code is divided into well‑marked sections for Extract, Transform, Load  
- Each function has a docstring explaining its purpose, inputs and outputs  
- Variable names are explicit instead of abbreviated  
- Progress messages are printed at every important stage of execution  
---
**Example Output Document (from MongoDB)**  
```
{
  "_id": {
    "$oid": "6898615047c50cf13534431e"
  },
  "id": 4,
  "games": [
    {
      "id": 3498,
      "slug": "grand-theft-auto-v",
      "name": "Grand Theft Auto V",
      "added": 22158
    },
    {
      "id": 3328,
      "slug": "the-witcher-3-wild-hunt",
      "name": "The Witcher 3: Wild Hunt",
      "added": 21817
    },
    {
      "id": 4200,
      "slug": "portal-2",
      "name": "Portal 2",
      "added": 20636
    },
    {
      "id": 4291,
      "slug": "counter-strike-global-offensive",
      "name": "Counter-Strike: Global Offensive",
      "added": 18292
    },
    {
      "id": 5286,
      "slug": "tomb-raider",
      "name": "Tomb Raider (2013)",
      "added": 17640
    },
    {
      "id": 13536,
      "slug": "portal",
      "name": "Portal",
      "added": 17594
    }
  ],
  "games_count": 553674,
  "image_background": "https://media.rawg.io/media/games/b8c/b8c243eaa0fbac8115e0cdccac3f91dc.jpg",
  "ingestion_timestamp": {
    "$date": "2025-08-10T09:07:28.058Z"
  },
  "name": "PC",
  "slug": "pc",
  "year_end": null,
  "year_start": null
}

```
---
**Project Structure**  
```
project-folder/
├── etl_connector.py        # The ETL Python script
├── .env                   # Secrets: API key, Mongo URI (not committed to git)
├── requirements.txt       # Python dependency list
├── README.md              # Project documentation
├── .gitignore             # Ensures .env and other sensitive files are not committed
```
---
**Summary**  
This pipeline securely connects to the RAWG API, extracts platform data, transforms it to keep only the most useful parts, and loads it into MongoDB. It has built‑in validation and error handling and is clearly written so it can be easily understood and maintained.  





