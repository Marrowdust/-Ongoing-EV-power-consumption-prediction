#download the data using the offical api for the acn dataset
import requests
import pandas as pd
import time
import random
import os
import json
from requests.exceptions import RequestException

# --- Checkpoint helpers ---
def save_checkpoint(checkpoint_data, file_path="checkpoint.json"):
    with open(file_path, "w") as f:
        json.dump(checkpoint_data, f)

def load_checkpoint(file_path="checkpoint.json"):
    try:
        with open(file_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def clear_checkpoint(file_path="checkpoint.json"):
    if os.path.exists(file_path):
        os.remove(file_path)

def load_existing_data(file_path="ev_charging_data_partial.csv"):
    try:
        df = pd.read_csv(file_path)
        return df.to_dict('records'), len(df)
    except FileNotFoundError:
        return [], 0

def save_partial_data(items, file_path="ev_charging_data_partial.csv"):
    if items:
        df = pd.DataFrame(items)
        df.to_csv(file_path, index=False)
        print(f"Saved {len(df)} items to {file_path}")

# --- Main function ---
def fetch_all_charging_data(site_id="caltech", include_ts=True, max_retries=5, backoff_factor=2):
    api_token = "olbNIFaxeWh_Mcpo0bET-NmCC_3KV-Z9hNRcMArC8J4"

    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }

    base_url = "https://ev.caltech.edu/api/v1"
    endpoint = f"{base_url}/sessions/{site_id}/ts" if include_ts else f"{base_url}/sessions/{site_id}"
    
    # Load checkpoint if available
    checkpoint_data = load_checkpoint()
    if checkpoint_data:
        print("Found checkpoint, resuming from previous session")
        current_url = checkpoint_data["next_url"]
        page_num = checkpoint_data["page_num"]
        # Load partial data
        all_items, items_count = load_existing_data()
        print(f"Loaded {items_count} existing records")
    else:
        current_url = endpoint
        page_num = 1
        all_items = []
    
    # Create a unique id set for deduplication
    item_ids = set(item.get('_id', '') for item in all_items)

    try:
        while current_url:
            retry_count = 0
            success = False

            while not success and retry_count < max_retries:
                try:
                    print(f"Fetching page {page_num}: {current_url}")
                    response = requests.get(current_url, headers=headers, timeout=30)

                    if response.status_code == 200:
                        success = True
                        data = response.json()

                        # Collect items with deduplication
                        if '_items' in data:
                            new_items = 0
                            for item in data['_items']:
                                if item.get('_id', '') not in item_ids:
                                    all_items.append(item)
                                    item_ids.add(item.get('_id', ''))
                                    new_items += 1
                            
                            print(f"Added {new_items} new items. Total so far: {len(all_items)}")

                        # Determine next URL
                        next_url = None
                        if '_links' in data and 'next' in data['_links']:
                            next_path = data['_links']['next']['href']
                            if next_path.startswith("http"):
                                next_url = next_path
                            else:
                                if next_path.startswith("/"):
                                    next_url = f"https://ev.caltech.edu{next_path}"
                                else:
                                    next_url = f"{base_url}/{next_path}"

                        # Save checkpoint and partial data every 10 pages or when there's no next page
                        if page_num % 10 == 0 or next_url is None:
                            checkpoint = {
                                "next_url": next_url,
                                "page_num": page_num + 1,
                                "total_items": len(all_items),
                                "timestamp": time.time()
                            }
                            save_checkpoint(checkpoint)
                            save_partial_data(all_items)

                        # Prepare for next page
                        current_url = next_url
                        page_num += 1

                        if next_url:
                            print(f"Next URL: {next_url}")
                        else:
                            print("No more pages")

                        # Be nice to server
                        time.sleep(1)

                    else:
                        print(f"Error: {response.status_code}")
                        print(response.text)

                        if response.status_code in [429, 500, 502, 503, 504]:
                            retry_count += 1
                            wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                            print(f"Retrying in {wait_time:.2f} seconds... (Attempt {retry_count}/{max_retries})")
                            time.sleep(wait_time)
                        else:
                            break

                except RequestException as e:
                    print(f"Request exception: {e}")
                    retry_count += 1
                    wait_time = backoff_factor * (2 ** retry_count) + random.uniform(0, 1)
                    print(f"Retrying in {wait_time:.2f} seconds... (Attempt {retry_count}/{max_retries})")
                    time.sleep(wait_time)

            if not success:
                print("Maximum retry attempts reached. Saving current progress...")
                checkpoint = {
                    "next_url": current_url,
                    "page_num": page_num,
                    "total_items": len(all_items),
                    "timestamp": time.time(),
                    "error": True
                }
                save_checkpoint(checkpoint)
                save_partial_data(all_items)
                break

    except KeyboardInterrupt:
        print("\nProcess interrupted by user. Saving progress...")
        checkpoint = {
            "next_url": current_url,
            "page_num": page_num,
            "total_items": len(all_items),
            "timestamp": time.time(),
            "interrupted": True
        }
        save_checkpoint(checkpoint)
        save_partial_data(all_items)

    # Convert to DataFrame
    if all_items:
        df = pd.DataFrame(all_items)
        print(f"\nTotal records retrieved: {len(df)}")
        print("\nDataFrame info:")
        print(df.info())
        print("\nFirst 5 rows:")
        print(df.head())

        # Clear the checkpoint only if we successfully completed
        if not current_url:
            clear_checkpoint()
            # Remove partial file if we have a complete dataset
            if os.path.exists("ev_charging_data_partial.csv"):
                os.remove("ev_charging_data_partial.csv")

        return df
    else:
        print("No data was retrieved.")
        return None

# --- Run it ---
if __name__ == "__main__":
    df = fetch_all_charging_data(site_id="caltech", include_ts=True)

    if df is not None and not df.empty:
        df.to_csv("ev_charging_data.csv", index=False)
        print("Data saved to ev_charging_data.csv")