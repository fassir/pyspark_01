import time
import json
import os
import requests
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# Configuration
# Switching to CoinGecko due to CoinCap Cloudflare blocking
API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false"
DATA_PATH = os.getenv("DATA_PATH", "./data/landing")

def fetch_data():
    try:
        session = requests.Session()
        retries = Retry(total=5, backoff_factor=2, status_forcelist=[ 429, 500, 502, 503, 504 ])
        session.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json'
        }

        response = session.get(API_URL, headers=headers, timeout=30)
        
        if response.status_code == 200:
            raw_data = response.json()
            # Transform to match CoinCap schema for downstream compatibility
            transformed_data = {
                "timestamp": int(time.time() * 1000),
                "data": []
            }
            for item in raw_data:
                transformed_data["data"].append({
                    "id": item.get("id"),
                    "symbol": item.get("symbol", "").upper(),
                    "name": item.get("name"),
                    "priceUsd": str(item.get("current_price", 0)),
                    "marketCapUsd": str(item.get("market_cap", 0)),
                    "volumeUsd24Hr": str(item.get("total_volume", 0)),
                    "changePercent24Hr": str(item.get("price_change_percentage_24h", 0))
                })
            return transformed_data
        else:
            print(f"Error: {response.status_code} - {response.text[:200]}")
            return None
    except Exception as e:
        print(f"Exception: {e}")
        return None

def save_data(data):
    if not data:
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"crypto_assets_{timestamp}.json"
    
    # Ensure directory exists
    os.makedirs(DATA_PATH, exist_ok=True)
    
    filepath = os.path.join(DATA_PATH, filename)
    
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"Saved data to {filepath}")

def main():
    print(f"Starting ingestion service (requests based)... Target: {DATA_PATH}")
    while True:
        print(f"[{datetime.now()}] Fetching data...")
        data = fetch_data()
        if data:
            save_data(data)
        
        # Fetch every 60 seconds
        time.sleep(60)

if __name__ == "__main__":
    main()
