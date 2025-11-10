import argparse
import boto3
import csv
import re
from datetime import datetime, date, time, timedelta
import pytz
import pandas as pd
import sys

# Hardcoded queues (your provided list)
HARDCODED_QUEUES = [
    "AggregationReadyUsers-nonprodqa", "AggregationReadyUsers-nonprodqa-01", 
    "AggregationReadyUsersPriority-nonprodqa", "AggregationReadyUsersPriority-nonprodqa-01", "GBUserDataIngestion-nonprodqa",
    "GBUserDataIngestionPriority-nonprodqa", "GbTempDataReadyEventPriority-nonprodqa",
    "GbTempDataReadyEventPyAmi-nonprodqa", "GbTempDataReadyEventPyAmi-nonprodqa-0", "GbTempDataReadyEventPyAmi-nonprodqa-00",
    "GbTempDataReadyEventPyAmi-nonprodqa-01", "GbTempDataReadyEventPyAmi-nonprodqa-1", "GbTempDataReadyEventPyAmiPriority-nonprodqa",
    "PDFGeneration-nonprodqa", "PDFGenerationPriority-nonprodqa"
]
AWS_REGION = 'us-west-2'


# --- Date parsing helpers ---
def _parse_ist_date_or_die(s: str) -> date:
    try:
        return datetime.strptime(s, "%d-%m-%Y").date()
    except ValueError:
        raise SystemExit("Invalid date format. Use dd-mm-yyyy, e.g., 13-10-2025")


def parse_args_ist_date_range():
    parser = argparse.ArgumentParser(description="SQS nonprodqa report (IST-based)")
    parser.add_argument("date", nargs="?", help="Single IST date in dd-mm-yyyy")
    parser.add_argument("--from", dest="from_date", help="Start IST date in dd-mm-yyyy")
    parser.add_argument("--to", dest="to_date", help="End IST date in dd-mm-yyyy")
    args = parser.parse_args()

    ist = pytz.timezone('Asia/Kolkata')

    if args.date and (args.from_date or args.to_date):
        raise SystemExit("Provide either a single date OR a --from/--to range, not both.")

    if args.date:
        d = _parse_ist_date_or_die(args.date)
        return d, d, ist

    if args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            raise SystemExit("Both --from and --to must be provided together.")
        start = _parse_ist_date_or_die(args.from_date)
        end = _parse_ist_date_or_die(args.to_date)
        if end < start:
            raise SystemExit("--to date must be the same or after --from date.")
        return start, end, ist

    today_ist = datetime.now(ist).date()
    return today_ist, today_ist, ist


def daterange(start_date, end_date):
    d = start_date
    while d <= end_date:
        yield d
        d += timedelta(days=1)


# --- Queue discovery ---
def get_all_sqs_queues():
    sqs_queues = set(HARDCODED_QUEUES)
    print(f"Using {len(sqs_queues)} queues.")
    return sorted(sqs_queues)


# --- Metric gathering ---
def get_sqs_metrics(queues, report_day_ist, ist_tz):
    cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)

    # Create 30-minute windows from 1:00 AM to 7:30 AM IST
    windows = []
    start_hour = 1
    start_minute = 0

    # Generate 13 windows: 1:00-1:30, 1:30-2:00, 2:00-2:30, ..., 7:00-7:30
    for i in range(13):
        window_start = ist_tz.localize(datetime.combine(report_day_ist, time(hour=start_hour, minute=start_minute)))

        # Calculate end time (30 minutes later)
        end_minute = start_minute + 30
        end_hour = start_hour
        if end_minute >= 60:
            end_minute = 0
            end_hour += 1

        window_end = ist_tz.localize(datetime.combine(report_day_ist, time(hour=end_hour, minute=end_minute)))

        # Create window label
        start_time_str = f"{start_hour:02d}:{start_minute:02d}"
        end_time_str = f"{end_hour:02d}:{end_minute:02d}"
        window_label = f"{start_time_str}-{end_time_str}"

        windows.append((window_label, window_start, window_end))

        # Move to next 30-minute slot
        start_minute += 30
        if start_minute >= 60:
            start_minute = 0
            start_hour += 1

    results = []

    for queue in queues:
        print(f"Processing queue: {queue}")
        row = {'Queue': queue}

        for win_label, start, end in windows:
            s_utc, e_utc = start.astimezone(pytz.utc), end.astimezone(pytz.utc)

            # Visible Messages
            try:
                vis_resp = cloudwatch.get_metric_statistics(
                    Namespace='AWS/SQS',
                    MetricName='ApproximateNumberOfMessagesVisible',
                    Dimensions=[{'Name': 'QueueName', 'Value': queue}],
                    StartTime=s_utc, EndTime=e_utc, Period=300, Statistics=['Maximum']
                )
                row[f'Visible_{win_label}'] = int(max([dp['Maximum'] for dp in vis_resp.get('Datapoints', [])], default=0))
            except Exception as e:
                row[f'Visible_{win_label}'] = f"Error: {str(e)}"

            # Received Messages
            try:
                rec_resp = cloudwatch.get_metric_statistics(
                    Namespace='AWS/SQS',
                    MetricName='NumberOfMessagesReceived',
                    Dimensions=[{'Name': 'QueueName', 'Value': queue}],
                    StartTime=s_utc, EndTime=e_utc, Period=300, Statistics=['Sum']
                )
                row[f'Received_{win_label}'] = int(sum([dp['Sum'] for dp in rec_resp.get('Datapoints', [])]))
            except Exception as e:
                row[f'Received_{win_label}'] = f"Error: {str(e)}"

        results.append(row)
    return results


# --- HTML output ---
def write_html(results_by_day, start_date, end_date):
    if start_date == end_date:
        filename = f"sqs_report_{start_date.strftime('%Y%m%d')}.html"
        title = f"SQS Report for {start_date.strftime('%d-%m-%Y')}"
    else:
        filename = f"sqs_report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.html"
        title = f"SQS Report from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}"

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 10px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            text-align: center;
            margin-bottom: 30px;
        }}
        h2 {{
            color: #555;
            border-bottom: 2px solid #007bff;
            padding-bottom: 5px;
            margin-top: 25px;
            font-size: 1.3rem;
        }}
        .table-container {{
            overflow-x: auto;
            margin-bottom: 30px;
            border: 1px solid #ddd;
            border-radius: 5px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            min-width: 1400px;
            font-size: 9px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 4px;
            text-align: left;
        }}
        th {{
            background-color: #007bff;
            color: white;
            font-weight: bold;
            text-align: center;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .queue-name {{
            font-weight: bold;
            color: #333;
        }}
        .metric-value {{
            text-align: right;
            font-family: monospace;
        }}
        .error {{
            color: #dc3545;
            font-style: italic;
        }}
        .summary {{
            background-color: #e9ecef;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            font-size: 0.9rem;
        }}

        /* Mobile-specific styles */
        @media (max-width: 768px) {{
            body {{
                padding: 5px;
            }}
            .container {{
                padding: 10px;
            }}
            h1 {{
                font-size: 1.4rem;
                margin-bottom: 15px;
            }}
            h2 {{
                font-size: 1.1rem;
                margin-top: 20px;
            }}
            table {{
                font-size: 8px;
                min-width: 1200px;
            }}
            th, td {{
                padding: 4px;
            }}
            .queue-name {{
                max-width: 120px;
                font-size: 9px;
            }}
            .summary {{
                padding: 10px;
                font-size: 0.8rem;
            }}
        }}

        @media (max-width: 480px) {{
            h1 {{
                font-size: 1.2rem;
            }}
            h2 {{
                font-size: 1rem;
            }}
            table {{
                font-size: 7px;
                min-width: 1000px;
            }}
            th, td {{
                padding: 3px;
            }}
            .queue-name {{
                max-width: 100px;
                font-size: 8px;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        <div class="summary">
            <strong>Report Generated:</strong> {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y %H:%M:%S IST')}<br>
            <strong>Time Windows:</strong> 30-minute intervals from 01:00 - 07:30 IST<br>
            • 13 windows: 01:00-01:30, 01:30-02:00, 02:00-02:30, ..., 07:00-07:30<br>
            • Each window shows Visible and Received message counts
        </div>
"""

    for day, results in results_by_day.items():
        df = pd.DataFrame(results)

        # Create column rename mapping for 30-minute windows
        rename_mapping = {}
        for col in df.columns:
            if col.startswith('Visible_') and col != 'Queue':
                time_window = col.replace('Visible_', '')
                rename_mapping[col] = f'Vis {time_window}'
            elif col.startswith('Received_') and col != 'Queue':
                time_window = col.replace('Received_', '')
                rename_mapping[col] = f'Rec {time_window}'

        df = df.rename(columns=rename_mapping)

        html_content += f"""
        <h2>📅 {day.strftime('%d-%m-%Y')}</h2>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
"""

        # Add table headers
        for col in df.columns:
            html_content += f"                    <th>{col}</th>\n"

        html_content += """                </tr>
            </thead>
            <tbody>
"""

        # Add table rows
        for _, row in df.iterrows():
            html_content += "                <tr>\n"
            for col in df.columns:
                value = row[col]
                if col == 'Queue':
                    html_content += f'                    <td class="queue-name">{value}</td>\n'
                elif isinstance(value, str) and value.startswith('Error:'):
                    html_content += f'                    <td class="error">{value}</td>\n'
                else:
                    html_content += f'                    <td class="metric-value">{value}</td>\n'
            html_content += "                </tr>\n"

        html_content += """            </tbody>
        </table>
"""

    html_content += """    </div>
</body>
</html>"""

    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)

    print(f"\n✅ HTML report saved: {filename}")
    print(f"📧 You can copy the HTML content from {filename} and paste it directly into your email.")

    return filename





# --- Main ---
if __name__ == "__main__":
    start_date, end_date, ist_tz = parse_args_ist_date_range()
    queues = get_all_sqs_queues()

    results_by_day = {}
    for d in daterange(start_date, end_date):
        print(f"\n=== Generating report for IST date: {d.strftime('%d-%m-%Y')} ===")
        results = get_sqs_metrics(queues, d, ist_tz)
        results_by_day[d] = results

    # Debug: Print summary of results
    total_queues_processed = 0
    for day, results in results_by_day.items():
        queue_count = len(results)
        total_queues_processed += queue_count
        print(f"Day {day.strftime('%d-%m-%Y')}: {queue_count} queues processed")

        # Print first few and last few queue names for verification
        if queue_count > 0:
            queue_names = [r['Queue'] for r in results]
            print(f"  First 3 queues: {queue_names[:3]}")
            print(f"  Last 3 queues: {queue_names[-3:]}")

    print(f"\nTotal queues processed across all days: {total_queues_processed}")

    write_html(results_by_day, start_date, end_date)
    print("\nAll done ✅")
