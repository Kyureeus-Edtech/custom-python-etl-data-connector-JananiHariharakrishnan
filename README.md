
## OTX Pulses ETL Pipeline (Python to MongoDB)
---
**Project Overview**  
This project implements an ETL (Extract – Transform – Load) process that retrieves cyber threat intelligence data from the AlienVault OTX (Open Threat Exchange) API, processes it to retain structured and relevant fields, and stores it in a MongoDB collection for further analysis or integration with security tools.

The script does the following:

- Extracts subscribed pulses from the OTX API using API key authentication, pagination, and optional filters (limit, modified_since)

- Transforms each pulse to include only useful threat intel metadata and indicators

- Loads the cleaned, structured pulses into MongoDB using upsert to avoid duplicates while updating existing entries

- Adds an ingestion timestamp for audit and data freshness tracking
---
**API Endpoint Details**  
Base URL: https://otx.alienvault.com/api/v1/pulses/subscribed
You must supply your API key in the HTTP request header: `X-OTX-API-KEY=YOUR_API_KEY`.  

Example endpoint:  
```
https://otx.alienvault.com/api/v1/pulses/subscribed?limit=10&page=1
```
---
**Common parameters**:  
- limit → Number of records per page (default: 10)

- page → Page number for pagination

- modified_since → (Optional) Only return pulses modified after a specific date (ISO 8601 format, e.g., 2025-01-01T00:00:00+00:00)

In this ETL we keep:  
- **id, name, author_name** – Unique ID of the pulse, human-readable title, and the pulse’s author.  
- **description** – Full pulse description, preserved exactly as provided by the OTX API.
- **created, modified** – ISO 8601 timestamps for when the pulse was first created and last updated.  
- **tags** – List of associated keywords for categorization.  
- **references** –  List of external URLs with further analysis or context.
- **targeted_countries** – Countries targeted by this threat campaign. 
- **indicators** –  A list of structured threat indicators (IOCs), each containing:

  - **indicator** – The actual value of the IoC (e.g., IP address, domain, URL, file hash).

  - **type** – Type of the IoC (e.g., IPv4, domain, URL, SHA256).

  - **title** – Short descriptive title or label for the indicator.

  - **description** – Additional descriptive context about the indicator.
- **ingestion_timestamp** – UTC timestamp of when the record was fetched and stored, used for auditing and data freshness tracking.
---
**Setup Instructions** 
1. Clone the repository and open the project folder.  

2. Create a `.env` file with the following content:  
```
OTX_API_KEY=your_otx_api_key_here
MONGO_URI=mongodb://localhost:27017/
DB_NAME=SSN_ETL_assignment
COLLECTION_NAME=otx_pulses_raw
LIMIT=10
MODIFIED_SINCE=
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
- raise_for_status() to detect and stop on HTTP errors (403, 429, etc.)

- Prints API error responses for debugging

- Skips database insertion if the API returns no results

- Uses upsert in MongoDB to update existing pulses without duplication

- Preserves full descriptions and indicator details for complete analysis

- Adds ingestion timestamps for tracking
---
**Why the Code is Self‑Documenting**  
- Function names clearly describe their purpose: fetch_pulses, transform_pulses, load_to_mongodb

- Extract / Transform / Load steps are separated into well-marked code blocks

- Explicit variable names (e.g., modified_since, transformed_pulses)

- Prints progress messages for every page and load operation


---
**Example Output Document (from MongoDB)**  
```
{
  "_id": {
    "$oid": "689a27c1ae36b3d182f47002"
  },
  "id": "6842f45696f96557e5f757b1",
  "author_name": "AlienVault",
  "created": "2025-06-06T13:59:50.252000",
  "description": "A sophisticated cyber campaign targeting China Mobile Tietong Co., Ltd., a subsidiary of China Mobile, has been uncovered. The operation, dubbed DRAGONCLONE, utilizes VELETRIX and VShell malware to infiltrate systems. The attack chain begins with a malicious ZIP file containing executable files and DLLs, exploiting DLL sideloading against Wondershare Repairit software. VELETRIX, a loader, employs anti-analysis techniques and IPFuscation to decode and execute VShell, a cross-platform OST framework. The campaign shows infrastructure overlaps with known China-nexus threat actors like UNC5174 and Earth Lamia. The attackers utilize various tools including Cobalt Strike, SuperShell, and Asset Lighthouse System for reconnaissance and post-exploitation activities.",
  "indicators": [
    {
      "indicator": "0668293c9f523f26babc09617063493b",
      "type": "FileHash-MD5",
      "title": "",
      "description": ""
    },
    {
      "indicator": "241f0748c8eec5bd7c6bf52a9a6ac1dd",
      "type": "FileHash-MD5",
      "title": "",
      "description": ""
    },
    {
      "indicator": "3199796dc2ad51da41da51de58d31012",
      "type": "FileHash-MD5",
      "title": "",
      "description": ""
    },
    {
      "indicator": "81f76f83d4c571fe95772f21aff4d0b9",
      "type": "FileHash-MD5",
      "title": "",
      "description": ""
    },
    {
      "indicator": "37a37bc7255089fdd000feb10780c2513c4416c8",
      "type": "FileHash-SHA1",
      "title": "",
      "description": ""
    },
    {
      "indicator": "ba8e2015fd0abe944d6b546088451ff05dd24849",
      "type": "FileHash-SHA1",
      "title": "",
      "description": ""
    },
    {
      "indicator": "ea97ee5f81f157e2ecf729b6c43f0997c3af20d3",
      "type": "FileHash-SHA1",
      "title": "",
      "description": ""
    },
    {
      "indicator": "f8cf927cb2baf893b136bc5d90535d193fc73b75",
      "type": "FileHash-SHA1",
      "title": "",
      "description": ""
    },
    {
      "indicator": "2206cc6bd9d15cf898f175ab845b3deb4b8627102b74e1accefe7a3ff0017112",
      "type": "FileHash-SHA256",
      "title": "",
      "description": ""
    },
    {
      "indicator": "40450b4212481492d2213d109a0cd0f42de8e813de42d53360da7efac7249df4",
      "type": "FileHash-SHA256",
      "title": "",
      "description": ""
    },
    {
      "indicator": "645f9f81eb83e52bbbd0726e5bf418f8235dd81ba01b6a945f8d6a31bf406992",
      "type": "FileHash-SHA256",
      "title": "",
      "description": ""
    },
    {
      "indicator": "a0f4ee6ea58a8896d2914176d2bfbdb9e16b700f52d2df1f77fe6ce663c1426a",
      "type": "FileHash-SHA256",
      "title": "",
      "description": ""
    },
    {
      "indicator": "ac6e0ee1328cfb1b6ca0541e4dfe7ba6398ea79a300c4019253bd908ab6a3dc0",
      "type": "FileHash-SHA256",
      "title": "",
      "description": ""
    },
    {
      "indicator": "ba4f9b324809876f906f3cb9b90f8af2f97487167beead549a8cddfd9a7c2fdc",
      "type": "FileHash-SHA256",
      "title": "",
      "description": ""
    },
    {
      "indicator": "bb6ab67ddbb74e7afb82bb063744a91f3fecf5fd0f453a179c0776727f6870c7",
      "type": "FileHash-SHA256",
      "title": "",
      "description": ""
    },
    {
      "indicator": "62.234.24.38",
      "type": "IPv4",
      "title": "",
      "description": ""
    },
    {
      "indicator": "156.238.236.130",
      "type": "IPv4",
      "title": "",
      "description": ""
    },
    {
      "indicator": "121.37.80.227",
      "type": "IPv4",
      "title": "",
      "description": ""
    },
    {
      "indicator": "fef69f8747c368979a9e4c62f4648ea233314b5f41981d9c01c1cdd96fb07365",
      "type": "FileHash-SHA256",
      "title": "",
      "description": ""
    }
  ],
  "ingestion_timestamp": {
    "$date": "2025-08-11T17:26:25.333Z"
  },
  "modified": "2025-08-11T17:15:56.021000",
  "name": "Operation DRAGONCLONE: Chinese Telecom Targeted by Malware",
  "references": [
    "https://www.seqrite.com/blog/operation-dragonclone-chinese-telecom-veletrix-vshell-malware/",
    "https://0x0d4y.blog/telecommunications-supply-chain-china-nexus-threat-technical-analysis-of-veletrix-loaders-strategic-infrastructure-positioning/"
  ],
  "tags": [
    "china-nexus",
    "veletrix",
    "dll sideloading",
    "earth lamia",
    "asset lighthouse system",
    "unc5174",
    "cve-2024-1709",
    "supershell",
    "cve-2025-31324",
    "cobalt strike"
  ],
  "targeted_countries": [
    "China"
  ]
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
This pipeline connects securely to the AlienVault OTX API, extracts subscribed pulses, transforms them into structured JSON documents, and loads them into MongoDB. It includes pagination, optional date filtering, and complete indicator details for security analysis. The process is designed for clarity, maintainability, and scalability — making it easy to integrate into broader threat intelligence workflows.



