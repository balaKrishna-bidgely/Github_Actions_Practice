import argparse
import boto3
import csv
import re
from datetime import datetime, date, time, timedelta
import pytz
import pandas as pd
import sys

# Excluded queues (unchanged)
EXCLUDED_QUEUES = [
    '90000S3BCSchedule-nonprodqa', '90000S3Invoice-nonprodqa', '90000S3RawData-nonprodqa', '90000S3UserEnrollment-nonprodqa',
    '90001S3BCSchedule-nonprodqa', '90001S3Invoice-nonprodqa', '90001S3RawData-nonprodqa', '90001S3UserEnrollment-nonprodqa',
    '90003S3BCSchedule-nonprodqa', '90003S3Invoice-nonprodqa', '90003S3RawData-nonprodqa', '90003S3UserEnrollment-nonprodqa',
    '90010S3BCSchedule-nonprodqa', '90010S3Invoice-nonprodqa', '90010S3RawData-nonprodqa', '90010S3UserEnrollment-nonprodqa',
    '90020S3BCSchedule-nonprodqa', '90020S3Invoice-nonprodqa', '90020S3RawData-nonprodqa', '90020S3UserEnrollment-nonprodqa',
    '90043S3BCSchedule-nonprodqa', '90043S3Invoice-nonprodqa', '90043S3RawData-nonprodqa', '90043S3UserEnrollment-nonprodqa',
    '90100S3BCSchedule-nonprodqa', '90100S3Invoice-nonprodqa', '90100S3RawData-nonprodqa', '90100S3UserEnrollment-nonprodqa',
    '90101S3BCSchedule-nonprodqa', '90101S3Invoice-nonprodqa', '90101S3RawData-nonprodqa', '90101S3UserEnrollment-nonprodqa',
    '90404S3BCSchedule-nonprodqa', '90404S3Invoice-nonprodqa', '90404S3RawData-nonprodqa', '90404S3UserEnrollment-nonprodqa',
    '90432S3BCSchedule-nonprodqa', '90432S3Invoice-nonprodqa', '90432S3RawData-nonprodqa', '90432S3UserEnrollment-nonprodqa',
    '90445S3BCSchedule-nonprodqa', 'AOSavingsEmailEvent-nonprodqa'
]

# Hardcoded queues (your provided list)
HARDCODED_QUEUES = [
    "AggregationReadyUsers-nonprodqa", "AggregationReadyUsers-nonprodqa-01", "AggregationReadyUsers-nonprodqa-adhoc",
    "AggregationReadyUsers-nonprodqa-nonprodqa", "AggregationReadyUsersDLQ-nonprodqa",
    "AggregationReadyUsersPriority-nonprodqa", "AggregationReadyUsersPriority-nonprodqa-01",
    "AggregationReadyUsersPriority-nonprodqa-02", "AggregationRetryUsers-nonprodqa", "BillProjectionEmailEvent-nonprodqa",
    "BillProjectionReadyPush-nonprodqa", "BillProjectionSMS-nonprodqa", "BudgetAlertEmailEvent-nonprodqa",
    "BudgetAlertSMS-nonprodqa", "EventAnalyser-nonprodqa", "EventAnalyserPriority-nonprodqa",
    "GBIngestionCompletionEvent-nonprodqa", "GBRawDataS3Event-nonprodqa", "GBUserDataIngestion-nonprodqa",
    "GBUserDataIngestionPriority-nonprodqa", "GBWelcomeEmailEvent-nonprodqa", "GBWelcomePush-nonprodqa",
    "GbDisaggReadyEvent-nonprodqa", "GbTempDataReadyEvent-nonprodqa", "GbTempDataReadyEventPriority-nonprodqa",
    "GbTempDataReadyEventPyAmi-nonprodqa", "GbTempDataReadyEventPyAmi-nonprodqa-0", "GbTempDataReadyEventPyAmi-nonprodqa-00",
    "GbTempDataReadyEventPyAmi-nonprodqa-01", "GbTempDataReadyEventPyAmi-nonprodqa-1", "GbTempDataReadyEventPyAmiPriority-nonprodqa",
    "GbUploadEvent-nonprodqa", "GbUploadEventPriority-nonprodqa", "GbcConsumptionEvent-nonprodqa",
    "GenericEventData-nonprodqa", "GenericNotificationEvent-nonprodqa", "MonthlySummaryEmailEvent-nonprodqa",
    "MonthlySummaryReadyPush-nonprodqa", "NBIEmailEvent-nonprodqa", "NbiDataReady-nonprodqa",
    "NeighbourhoodComparisonEmailEvent-nonprodqa", "NeighbourhoodComparisonEmailrandomEvent-nonprodqa",
    "NeighbourhoodComparisonEvent-nonprodqa", "NotificationsProcessorEventPriority-nonprodqa",
    "PDFGeneration-nonprodqa", "PDFGenerationPriority-nonprodqa", "PdfGeneration-nonprodqa",
    "PdfGenerationPriority-nonprodqa", "RateComparison-nonprodqa", "RatePlanEvent-nonprodqa",
    "S3RawDataArchiveEvent-nonprodqa",
]


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
    candidates = set(HARDCODED_QUEUES)
    filtered = [q for q in candidates if 'nonprodqa' in q.lower() and not re.match(r'^\d', q)]
    excluded = {q.lower() for q in EXCLUDED_QUEUES}
    final = [q for q in filtered if q.lower() not in excluded]
    print(f"Using {len(final)} queues after filtering/exclusions.")
    return sorted(final)


# --- Metric gathering ---
def get_sqs_metrics(queues, report_day_ist, ist_tz):
    cloudwatch = boto3.client('cloudwatch', region_name='us-west-2')
    start1 = ist_tz.localize(datetime.combine(report_day_ist, time(hour=1, minute=30)))
    end1 = ist_tz.localize(datetime.combine(report_day_ist, time(hour=7, minute=30)))
    start2 = end1
    end2 = ist_tz.localize(datetime.combine(report_day_ist, time(hour=9, minute=0)))

    windows = [
        ('Window1', start1, end1),
        ('Window2', start2, end2)
    ]
    results = []

    for queue in queues:
        print(f"Processing queue: {queue}")
        row = {'Queue': queue}

        for win, start, end in windows:
            s_utc, e_utc = start.astimezone(pytz.utc), end.astimezone(pytz.utc)

            # Visible
            try:
                vis_resp = cloudwatch.get_metric_statistics(
                    Namespace='AWS/SQS',
                    MetricName='ApproximateNumberOfMessagesVisible',
                    Dimensions=[{'Name': 'QueueName', 'Value': queue}],
                    StartTime=s_utc, EndTime=e_utc, Period=300, Statistics=['Maximum']
                )
                row[f'Visible_{win}'] = int(max([dp['Maximum'] for dp in vis_resp.get('Datapoints', [])], default=0))
            except Exception as e:
                row[f'Visible_{win}'] = f"Error: {str(e)}"

            # Received
            try:
                rec_resp = cloudwatch.get_metric_statistics(
                    Namespace='AWS/SQS',
                    MetricName='NumberOfMessagesReceived',
                    Dimensions=[{'Name': 'QueueName', 'Value': queue}],
                    StartTime=s_utc, EndTime=e_utc, Period=300, Statistics=['Sum']
                )
                row[f'Received_{win}'] = int(sum([dp['Sum'] for dp in rec_resp.get('Datapoints', [])]))
            except Exception as e:
                row[f'Received_{win}'] = f"Error: {str(e)}"

            # Deleted (only Window2)
            if win == 'Window2':
                try:
                    del_resp = cloudwatch.get_metric_statistics(
                        Namespace='AWS/SQS',
                        MetricName='NumberOfMessagesDeleted',
                        Dimensions=[{'Name': 'QueueName', 'Value': queue}],
                        StartTime=s_utc, EndTime=e_utc, Period=300, Statistics=['Sum']
                    )
                    row['Deleted_Window2'] = int(sum([dp['Sum'] for dp in del_resp.get('Datapoints', [])]))
                except Exception as e:
                    row['Deleted_Window2'] = f"Error: {str(e)}"

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
            min-width: 600px;
            font-size: 11px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 8px;
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
                font-size: 10px;
                min-width: 500px;
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
                font-size: 9px;
                min-width: 450px;
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
            <strong>Time Windows:</strong><br>
            • Window 1: 01:30 - 07:30 IST<br>
            • Window 2: 07:30 - 09:00 IST
        </div>
"""

    for day, results in results_by_day.items():
        df = pd.DataFrame(results)
        df = df.rename(columns={
            'Visible_Window1': 'Visible (1:30-7:30)',
            'Received_Window1': 'Received (1:30-7:30)',
            'Visible_Window2': 'Visible (7:30-9:00)',
            'Received_Window2': 'Received (7:30-9:00)',
            'Deleted_Window2': 'Deleted (7:30-9:00)'
        })

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


# --- Email-friendly HTML output ---
def write_email_html(results_by_day, start_date, end_date):
    """Generate email-friendly HTML content without full HTML structure"""
    if start_date == end_date:
        title = f"SQS Report for {start_date.strftime('%d-%m-%Y')}"
    else:
        title = f"SQS Report from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}"

    # Inline CSS for email compatibility with mobile responsiveness
    email_html = f'''
<div style="font-family: Arial, sans-serif; margin: 0; padding: 10px; background-color: #f5f5f5; line-height: 1.4;">
    <div style="max-width: 1200px; margin: 0 auto; background-color: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
        <h1 style="color: #333; text-align: center; margin-bottom: 20px; font-size: 1.6rem;">{title}</h1>
        <div style="background-color: #e9ecef; padding: 15px; border-radius: 5px; margin-bottom: 20px; font-size: 0.9rem;">
            <strong>Report Generated:</strong> {datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y %H:%M:%S IST')}<br>
            <strong>Time Windows:</strong><br>
            • Window 1: 01:30 - 07:30 IST<br>
            • Window 2: 07:30 - 09:00 IST
        </div>
'''

    for day, results in results_by_day.items():
        df = pd.DataFrame(results)
        df = df.rename(columns={
            'Visible_Window1': 'Visible (1:30-7:30)',
            'Received_Window1': 'Received (1:30-7:30)',
            'Visible_Window2': 'Visible (7:30-9:00)',
            'Received_Window2': 'Received (7:30-9:00)',
            'Deleted_Window2': 'Deleted (7:30-9:00)'
        })

        email_html += f'''
        <h2 style="color: #555; border-bottom: 2px solid #007bff; padding-bottom: 5px; margin-top: 25px; font-size: 1.2rem;">📅 {day.strftime('%d-%m-%Y')}</h2>
        <div style="overflow-x: auto; margin-bottom: 25px; border: 1px solid #ddd; border-radius: 5px;">
            <table style="width: 100%; border-collapse: collapse; min-width: 500px; font-size: 11px;">
                <thead>
                    <tr>
'''

        # Add table headers
        for col in df.columns:
            email_html += f'                        <th style="border: 1px solid #ddd; padding: 6px; background-color: #007bff; color: white; font-weight: bold; text-align: center; white-space: nowrap;">{col}</th>\n'

        email_html += '''                </tr>
            </thead>
            <tbody>
'''

        # Add table rows
        for i, (_, row) in enumerate(df.iterrows()):
            bg_color = "#f9f9f9" if i % 2 == 1 else "white"
            email_html += f'                <tr style="background-color: {bg_color};">\n'
            for col in df.columns:
                value = row[col]
                if col == 'Queue':
                    email_html += f'                        <td style="border: 1px solid #ddd; padding: 6px; font-weight: bold; color: #333; max-width: 150px; word-wrap: break-word; white-space: normal; font-size: 10px;">{value}</td>\n'
                elif isinstance(value, str) and value.startswith('Error:'):
                    email_html += f'                        <td style="border: 1px solid #ddd; padding: 6px; color: #dc3545; font-style: italic; white-space: nowrap;">{value}</td>\n'
                else:
                    email_html += f'                        <td style="border: 1px solid #ddd; padding: 6px; text-align: right; font-family: monospace; white-space: nowrap;">{value}</td>\n'
            email_html += "                    </tr>\n"

        email_html += '''            </tbody>
        </table>
'''

    email_html += '''    </div>
</div>'''

    # Save email-friendly version
    if start_date == end_date:
        email_filename = f"sqs_report_email_{start_date.strftime('%Y%m%d')}.html"
    else:
        email_filename = f"sqs_report_email_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.html"

    with open(email_filename, 'w', encoding='utf-8') as f:
        f.write(email_html)

    print(f"📧 Email-friendly HTML saved: {email_filename}")
    return email_filename


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
    email_file = write_email_html(results_by_day, start_date, end_date)

    # Debug: Verify email file content
    if email_file:
        import os
        file_size = os.path.getsize(email_file)
        print(f"Email HTML file size: {file_size} bytes")

        # Count queues in email file
        with open(email_file, 'r', encoding='utf-8') as f:
            email_content = f.read()
            queue_count_in_email = email_content.count('<td style="border: 1px solid #ddd; padding: 8px; font-weight: bold; color: #333;">')
            print(f"Queues found in email HTML: {queue_count_in_email}")

    print("\nAll done ✅")
