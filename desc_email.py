import concurrent.futures
import time
import os
import requests
import logging
import csv
from datetime import datetime
from bs4 import BeautifulSoup
import re

# --- Global Configuration ---
API_BASE_URL = "https://naapi2-external.bidgely.com"
API_ACCESS_TOKEN = "0139af95-db44-4568-9444-a544e9b484e3"
ID = "TOU_RATE_PROMOTION" #in email body TOU_BILL_SUGGESTION for kudo savings amt
NOTIFICATION_TYPES = ['MONTHLY_SUMMARY','BILL_PROJECTION']
# --------------------------

def setup_logger():
    """Sets up the main console logger."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def get_suggestion_from_notification_body(notification_id: str) -> str:
    """
    Fetches notification details and parses the HTML body to find a suggestion.
    It finds an element with id 'TOU_RATE_PROMOTION', and then within that
    element, it finds the text of an element with class 'content-head'.
    
    Args:
        notification_id: The ID of the notification to fetch.
        
    Returns:
        The extracted text or a status message if not found.
    """
    try:
        notificationId_api_url = f"{API_BASE_URL}/2.1/utility_notifications/notifications/{notification_id}?access_token={API_ACCESS_TOKEN}"
        response = requests.get(notificationId_api_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        notification_body_html = data.get("payload", {}).get("notificationBody")

        if not notification_body_html:
            return "Notification body was empty."

        soup = BeautifulSoup(notification_body_html, 'html.parser')
        
        container_element = soup.find(id=ID)
        
        if container_element:
            content_head_element = container_element.find(class_='content-head')
            if content_head_element:
                full_text = content_head_element.get_text(strip=True)
                match = re.search(r'(\d+)', full_text) # Return the matched string, e.g., "$30"
                if match:
                    return match.group(0)
                else:
                    return full_text
            else:
                return f"Found '{ID}' but no 'content-head' class inside."
        else:
            return ""  #f"Element with ID '{ID}' not found."

    except requests.exceptions.RequestException as e:
        logging.warning(f"Could not fetch details for notification {notification_id}: {e}")
        return "Failed to fetch details."
    except Exception as e:
        logging.warning(f"Error processing details for notification {notification_id}: {e}")
        return "Error processing details."


def process_user(user_id: str) -> list:
    """
    Makes an API call, finds relevant notifications, and returns them as a list of rows.
    
    Returns:
        A list of lists, where each inner list is a row for the CSV file.
    """
    api_url = f"{API_BASE_URL}/2.1/utility_notifications/users/{user_id}?access_token={API_ACCESS_TOKEN}"
    found_rows = []

    try:
        response = requests.get(api_url, timeout=15)
        response.raise_for_status()
        data = response.json()

        if data.get('payload', {}).get('totalCount', 0) > 0:
            notifications_list = data.get('payload', {}).get('notificationsList', [])

            for notification in notifications_list:
                if notification.get('notificationType') in NOTIFICATION_TYPES:
                    timestamp_ms = notification.get('generationTimestamp', 0)
                    gen_date = datetime.fromtimestamp(timestamp_ms / 1000)
                    
                    if gen_date.year == 2025 and gen_date.month in [5, 6, 7, 8]:
                        notification_id = notification.get('notificationId')
                        if notification_id:
                            suggestion_text = get_suggestion_from_notification_body(notification_id)
                            # Get the full month name (e.g., "May")
                            month_name = gen_date.strftime('%B')
                            
                            # Prepare the row for the CSV file
                            row = [
                                user_id,
                                notification.get('notificationType'),
                                notification_id,
                                timestamp_ms,
                                month_name, # Added Month Name column
                                suggestion_text
                            ]
                            if suggestion_text != "":
                                logging.info(f"found: {row}")
                            found_rows.append(row)

        else:
            logging.info(f"ℹ️  User {user_id} has 0 total notifications.")

        # Return a list of all found rows for this user
        return ["Success", found_rows]

    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error for user {user_id}: {http_err}")
        return ["Failed", []]
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Request error for user {user_id}: {req_err}")
        return ["Failed", []]
    except Exception as e:
        logging.error(f"An unexpected error occurred for user {user_id}: {e}")
        return ["Failed", []]

def process_users_from_file(file_path: str, output_csv_path: str, max_threads: int = None):
    """
    Reads user IDs, processes them in parallel, and writes results to a CSV file.
    """
    if not os.path.exists(file_path):
        logging.error(f"File not found at '{file_path}'")
        return

    unique_user_ids = set()
    logging.info(f"Reading user IDs from '{file_path}'...")
    try:
        with open(file_path, 'r') as f:
            for line in f:
                user_id = line.strip()
                if user_id:
                    unique_user_ids.add(user_id)
    except Exception as e:
        logging.error(f"Error reading file: {e}")
        return

    if not unique_user_ids:
        logging.warning("No unique user IDs found to process.")
        return

    total_users = len(unique_user_ids)
    logging.info(f"Found {total_users} unique user IDs to process.")

    if max_threads is None:
        cpu_cores = os.cpu_count() or 1
        max_threads = min(total_users, cpu_cores * 2)

    logging.info(f"Starting processor with up to {max_threads} threads...")
    logging.info(f"Results will be saved to '{output_csv_path}'")
    start_time = time.time()

    all_results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_threads) as executor:
        all_results = list(executor.map(process_user, unique_user_ids))

    # --- Write results to CSV ---
    success_count = 0
    with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        # Write header
        writer.writerow(['userId', 'notificationType', 'notificationId', 'generationTimestamp', 'Month', 'extractedText'])
        
        # Write data rows
        for status, rows in all_results:
            if status == "Success":
                success_count += 1
            if rows:
                writer.writerows(rows)
    
    end_time = time.time()
    failed_count = total_users - success_count
    
    logging.info("-" * 40)
    logging.info("🎉 Processing Complete!")
    logging.info(f"Total Users Processed: {total_users}")
    logging.info(f"✅ Successful API Calls: {success_count}")
    logging.info(f"❌ Failed API Calls: {failed_count}")
    logging.info(f"Total time taken: {end_time - start_time:.2f} seconds")
    logging.info(f"Results have been saved to '{output_csv_path}'")
    logging.info("-" * 40)

def main():
    """
    Main function to configure and run the script.
    """
    setup_logger()

    USER_FILE_PATH = "users.csv"
    OUTPUT_CSV_PATH = "output.csv"

    process_users_from_file(
        file_path=USER_FILE_PATH,
        output_csv_path=OUTPUT_CSV_PATH
    )

if __name__ == "__main__":
    main()
