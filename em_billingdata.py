import pandas as pd
import requests
import time
import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv

# Parse command-line arguments
parser = argparse.ArgumentParser(description="Billing Data Fetch Script")
parser.add_argument('--input', required=True, help="Input file containing UUIDs (e.g., users.txt)")
parser.add_argument("--start", type=int, required=True, help="Start line (1-based index)")
parser.add_argument("--end", type=int, required=True, help="End line (inclusive, 1-based index)")
parser.add_argument('--output', required=True, help="Output CSV file name (e.g., billing_data.csv)")
# parser.add_argument('--failures', required=False, help="Failures CSV file name (e.g., failures.csv)")
args = parser.parse_args()

input_file = args.input
output_file = args.output
# failures_file = args.failures

# Read UUIDs
df1 = pd.read_csv(input_file, header=None, names=['uuid'])
uuids = df1['uuid'].tolist()

# Slice by start/end (1-based indexing)
start = max(args.start, 1) - 1  # convert to 0-based
end = min(args.end, len(uuids))  # inclusive end
uuids = uuids[start:end]

print(f"📌 Processing {len(uuids)} UUIDs (lines {args.start}-{args.end}) from {input_file}")

# API Headers
headers = {
    "Authorization": "bearer 9a117dd5-9373-4825-a724-e862e852beb7",
    "Content-Type": "application/json"
}

results = []
failed_uuids = []
results_lock = Lock()
failures_lock = Lock()
log_lock = Lock()

# Multithreaded processing
max_workers = 30

def create_session(max_pool_size):
    """Create a requests session with retry & backoff."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=max_pool_size, pool_maxsize=max_pool_size)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


# Use a shared session for performance & retries
SESSION = create_session(max_workers)

def fetch(uuid, max_retries=5):
    url = f"https://caapi.bidgely.com/billingdata/users/{uuid}/homes/1/utilitydata?t0=1&t1=2110163358"

    for attempt in range(max_retries):
        try:
            res = SESSION.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                data = res.json()
                if not data:
                    print(f"Empty response for user: {uuid}")
                    continue

                user_row = {"uuid": uuid}
                user_valid = True

                # Sort by billingStartTs
                for i, (ts, bill) in enumerate(
                    sorted(data.items(), key=lambda x: x[1].get("billingStartTs", 0)),
                    start=1,
                ):
                    invoice_list = bill.get("invoiceDataList", [])
                    charge_map = {inv.get("chargeType"): inv for inv in invoice_list}

                    if "BB_AMOUNT" in charge_map and "TOTAL" in charge_map:
                        bb = charge_map["BB_AMOUNT"]
                        total = charge_map["TOTAL"]

                        # Flatten with chargeName + chargeType + cost
                        user_row[f"billingStartTs_{i}"] = bill.get("billingStartTs")
                        user_row[f"billingEndTs_{i}"] = bill.get("billingEndTs")

                        user_row[f"BB_AMOUNT_chargeName_{i}"] = bb.get("chargeName")
                        user_row[f"BB_AMOUNT_chargeType_{i}"] = bb.get("chargeType")
                        user_row[f"BB_AMOUNT_cost_{i}"] = bb.get("cost")

                        user_row[f"TOTAL_chargeName_{i}"] = total.get("chargeName")
                        user_row[f"TOTAL_chargeType_{i}"] = total.get("chargeType")
                        user_row[f"TOTAL_cost_{i}"] = total.get("cost")
                    else:
                        user_valid = False
                        break  # skip this user if any cycle missing

                if user_valid:
                    with results_lock:
                        results.append(user_row)
                else:
                    with failures_lock:
                        failed_uuids.append(uuid)
                return
            else:
                raise requests.RequestException(f"Bad status code: {res.status_code}")
        except Exception as e:
            wait_time = 3 ** attempt
            with log_lock:
                print(f"⚠️ Attempt {attempt+1} failed for UUID {uuid}: {e}. Retrying in {wait_time}s...\n")
            time.sleep(wait_time)

    with failures_lock:
        failed_uuids.append({'uuid': uuid})
    with log_lock:
        print(f"❌ All retries failed for UUID {uuid}\n")

with ThreadPoolExecutor(max_workers=max_workers) as executor:
    futures = {executor.submit(fetch, uuid): uuid for uuid in uuids}
    for _ in as_completed(futures):
        pass  # wait for all to complete

# Write results
if results:
    df = pd.DataFrame(results)
    if os.path.exists(output_file):
        df.to_csv(output_file, mode='a', header=False, index=False)
    else:
        df.to_csv(output_file, index=False)
    print(f"\n✅ Processed {len(results)} UUIDs. Appended results to '{output_file}'.")
else:
    print("\n⚠️ No valid results to write.")

# Write failures if requested
if failed_uuids:
    failed_csv = output_file.replace(".csv", "_failed.csv")
    with open(failed_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["userId"])
        for uid in failed_uuids:
            writer.writerow([uid])
    print(f"❌ {len(failed_uuids)} UUIDs failed. Saved to '{failed_csv}'.\n")
    print(failed_uuids)
else:
    print("🎉 No UUIDs failed.")
