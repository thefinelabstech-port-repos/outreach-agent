import asyncio
from sheets import SheetsManager
from apify_client import ApifyClient

def debug_apify():
    manager = SheetsManager("credentials.json")
    keys = manager.get_apify_keys("https://docs.google.com/spreadsheets/d/1uRxPzWt5JH7gb5Kz0mqM4oCeink6h73tjF3_q641bI8/edit?gid=0#gid=0")
    print(f"Keys found: {len(keys)}")
    
    # Try the first key
    key = keys[0]
    client = ApifyClient(key)
    
    run_input = {
        "profileScraperMode": "Full",
        "searchQuery": "Founders",
        "maxItems": 1,
        "industryIds": ["11"],
        "startPage": 1,
        "locations": ["Bangalore"]
    }
    print("Running...")
    run = client.actor("M2FMdjRVeF1HPGFcc").call(run_input=run_input)
    print(f"Run status: {run.get('status')}")
    print(f"Run status message: {run.get('statusMessage')}")
    print(f"Run keys: {list(run.keys())}")

if __name__ == "__main__":
    debug_apify()
