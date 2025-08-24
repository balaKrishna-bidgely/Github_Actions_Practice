import concurrent.futures
import time
import os
import requests
import logging
import csv
from datetime import datetime
from bs4 import BeautifulSoup
import re
import argparse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- Global Configuration ---
API_BASE_URL = "https://naapi2-external.bidgely.com"
API_ACCESS_TOKEN = "e4b98e74-ccab-49ee-819a-81005a8302e4"
ID = "TOU_RATE_PROMOTION"
MONTHS_TO_CHECK = [5, 6, 7, 8]
NOTIFICATION_TYPES = ['MONTHLY_SUMMARY', 'BILL_PROJECTION']
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}
THREAD_POOL_SIZE = 100
# ----------------------------

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True
    )

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
SESSION = create_session(THREAD_POOL_SIZE)
logging.info(f"Considering connection pool of {THREAD_POOL_SIZE}")

def get_suggestion_from_notification_body(notification_id: str) -> str:
    try:
        url = f"{API_BASE_URL}/2.1/utility_notifications/notifications/{notification_id}?access_token={API_ACCESS_TOKEN}"
        response = SESSION.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        data = response.json()

        html_body = data.get("payload", {}).get("notificationBody")
        if not html_body:
            return ""

        soup = BeautifulSoup(html_body, "html.parser")
        container = soup.find(id=ID)
        if not container:
            return ""

        content = container.find(class_="content-head")
        if not content:
            return ""

        full_text = content.get_text(strip=True)
        match = re.search(r"(\d+)", full_text)
        return match.group(0) if match else full_text

    except Exception as e:
        logging.warning(f"Notification {notification_id} fetch failed: {e}")
        return ""

def process_user(user_id: str) -> list:
    logging.info(f"processing user: {user_id}")
    url = f"{API_BASE_URL}/2.1/utility_notifications/users/{user_id}?access_token={API_ACCESS_TOKEN}"
    found_rows = []
    try:
        response = SESSION.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        data = response.json()

        if data.get("payload", {}).get("totalCount", 0) > 0:
            for n in data.get("payload", {}).get("notificationsList", []):
                if n.get("notificationType") in NOTIFICATION_TYPES:
                    ts_ms = n.get("generationTimestamp", 0)
                    gen_date = datetime.fromtimestamp(ts_ms / 1000)

                    if gen_date.year == 2025 and gen_date.month in MONTHS_TO_CHECK:
                        nid = n.get("notificationId")
                        if nid:
                            suggestion = get_suggestion_from_notification_body(nid)
                            row = [
                                user_id,
                                n.get("notificationType"),
                                nid,
                                ts_ms,
                                gen_date.strftime("%B").upper(),
                                suggestion
                            ]
                            logging.info(f"Found: {row}")
                            found_rows.append(row)
        else:
            logging.info(f"user: {user_id} has 0 notifications!")
        return ["Success", found_rows]

    except Exception as e:
        logging.error(f"Error for user {user_id}: {e}")
        return ["Failed", []]

def read_user_ids(file_path, start, end):
    users = []
    with open(file_path, "r") as f:
        for i, line in enumerate(f, start=1):
            if start <= i <= end:
                users.append(line.strip())
            elif i > end:
                break
    return users

def process_users_from_file(file_path, output_csv, start, end, max_threads=THREAD_POOL_SIZE):
    users = read_user_ids(file_path, start, end)
    total = len(users)
    THREAD_POOL_SIZE = min(total, max_threads)
    logging.info(f"Considering thread-pool size {THREAD_POOL_SIZE}")
    logging.info(f"Processing {total} users (lines {start}-{end})...")

    success_count = 0
    fail_count = 0
    failed_users = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        results_iter = executor.map(process_user, users)

        with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["userId", "notificationType", "notificationId", "generationTimestamp", "Month", "loadShiftAmount"])

            processed = 0
            for (status, rows), user_id in zip(results_iter, users):
                processed += 1
                if status == "Success":
                    success_count += 1
                else:
                    fail_count += 1
                    failed_users.append(user_id)

                if rows:
                    writer.writerows(rows)

                if processed % 1000 == 0:
                    logging.info(f"Processed {processed}/{total} users...")

    logging.info(f"✅ Finished batch {start}-{end}, saved to {output_csv}")
    logging.info(f"📊 Summary: Total={total}, Success={success_count}, Failed={fail_count}")

    if failed_users:
        failed_csv = output_csv.replace(".csv", "_failed.csv")
        with open(failed_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["userId"])
            for uid in failed_users:
                writer.writerow([uid])
        logging.info(f"💾 Failed userIds saved to {failed_csv}")

def main():
    setup_logger()
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to user file")
    parser.add_argument("--start", type=int, required=True, help="Start line")
    parser.add_argument("--end", type=int, required=True, help="End line")
    parser.add_argument("--output", required=True, help="Output CSV path")
    args = parser.parse_args()

    process_users_from_file(args.input, args.output, args.start, args.end)

if __name__ == "__main__":
    main()