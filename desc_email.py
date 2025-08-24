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
import logging

# --- Global Configuration ---
API_BASE_URL = "https://naapi2-external.bidgely.com"
API_ACCESS_TOKEN = "e4b98e74-ccab-49ee-819a-81005a8302e4"
ID = "TOU_RATE_PROMOTION"
NOTIFICATION_TYPES = ['MONTHLY_SUMMARY','BILL_PROJECTION']
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
}
# ----------------------------

def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

def get_suggestion_from_notification_body(notification_id: str) -> str:
    try:
        url = f"{API_BASE_URL}/2.1/utility_notifications/notifications/{notification_id}?access_token={API_ACCESS_TOKEN}"
        response = requests.get(url, headers=HEADERS, timeout=30)
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
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()

        if data.get("payload", {}).get("totalCount", 0) > 0:
            for n in data.get("payload", {}).get("notificationsList", []):
                if n.get("notificationType") in NOTIFICATION_TYPES:
                    ts_ms = n.get("generationTimestamp", 0)
                    gen_date = datetime.fromtimestamp(ts_ms / 1000)

                    if gen_date.year == 2025 and gen_date.month in [5,6,7,8]:
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
            logging.info(f"user: {user_id} has 0 noitifactions!")
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

def process_users_from_file(file_path, output_csv, start, end, max_threads=100):
    users = read_user_ids(file_path, start, end)
    total = len(users)
    logging.info(f"Processing {total} users (lines {start}-{end})...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        results_iter = executor.map(process_user, users)

        with open(output_csv, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["userId","notificationType","notificationId","generationTimestamp","Month","loadShiftAmount"])

            processed = 0
            for status, rows in results_iter:
                processed += 1
                if rows:
                    writer.writerows(rows)

                if processed % 1000 == 0:
                    logging.info(f"Processed {processed}/{total} users...")

    logging.info(f"✅ Finished batch {start}-{end}, saved to {output_csv}")

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