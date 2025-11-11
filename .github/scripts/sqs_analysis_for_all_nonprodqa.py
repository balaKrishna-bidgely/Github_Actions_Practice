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
    "GbTempDataReadyEventPyAmi-nonprodqa", "GbTempDataReadyEventPyAmi-nonprodqa-00",
    "GbTempDataReadyEventPyAmi-nonprodqa-01", "GbTempDataReadyEventPyAmiPriority-nonprodqa"
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
    parser.add_argument('--start-time', dest='start_time', default='01:00',
                       help='Start time in HH:MM format (default: 01:00)')
    parser.add_argument('--end-time', dest='end_time', default='07:30',
                       help='End time in HH:MM format (default: 07:30)')
    parser.add_argument('--granularity', dest='granularity', type=int, default=30,
                       help='Time window granularity in minutes (default: 30)')
    args = parser.parse_args()

    ist = pytz.timezone('Asia/Kolkata')

    # Parse and validate time arguments
    try:
        start_hour, start_minute = map(int, args.start_time.split(':'))
        end_hour, end_minute = map(int, args.end_time.split(':'))

        if not (0 <= start_hour <= 23 and 0 <= start_minute <= 59):
            raise ValueError("Invalid start time")
        if not (0 <= end_hour <= 23 and 0 <= end_minute <= 59):
            raise ValueError("Invalid end time")
        if args.granularity <= 0 or args.granularity > 1440:  # Max 24 hours
            raise ValueError("Granularity must be between 1 and 1440 minutes")

    except ValueError as e:
        print(f"Error parsing time arguments: {e}")
        print("Time format should be HH:MM (e.g., 01:00, 07:30)")
        raise SystemExit("Invalid time parameters")

    if args.date and (args.from_date or args.to_date):
        raise SystemExit("Provide either a single date OR a --from/--to range, not both.")

    if args.date:
        d = _parse_ist_date_or_die(args.date)
        return d, d, ist, (start_hour, start_minute), (end_hour, end_minute), args.granularity

    if args.from_date or args.to_date:
        if not (args.from_date and args.to_date):
            raise SystemExit("Both --from and --to must be provided together.")
        start = _parse_ist_date_or_die(args.from_date)
        end = _parse_ist_date_or_die(args.to_date)
        if end < start:
            raise SystemExit("--to date must be the same or after --from date.")
        return start, end, ist, (start_hour, start_minute), (end_hour, end_minute), args.granularity

    today_ist = datetime.now(ist).date()
    return today_ist, today_ist, ist, (start_hour, start_minute), (end_hour, end_minute), args.granularity


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
def get_sqs_metrics(queues, report_day_ist, ist_tz, start_time, end_time, granularity):
    cloudwatch = boto3.client('cloudwatch', region_name=AWS_REGION)

    # Create time windows based on provided parameters
    windows = []
    start_hour, start_minute = start_time
    end_hour, end_minute = end_time

    # Calculate total minutes and number of windows
    start_total_minutes = start_hour * 60 + start_minute
    end_total_minutes = end_hour * 60 + end_minute

    # Handle case where end time is next day (e.g., 23:00 to 01:00)
    if end_total_minutes <= start_total_minutes:
        end_total_minutes += 24 * 60  # Add 24 hours

    total_duration = end_total_minutes - start_total_minutes
    num_windows = total_duration // granularity

    print(f"Generating {num_windows} windows of {granularity} minutes each from {start_hour:02d}:{start_minute:02d} to {end_hour:02d}:{end_minute:02d}")

    # Generate windows dynamically
    current_minutes = start_total_minutes
    for i in range(num_windows):
        # Calculate current window start time
        window_start_hour = (current_minutes // 60) % 24
        window_start_minute = current_minutes % 60

        # Calculate window end time
        end_minutes = current_minutes + granularity
        window_end_hour = (end_minutes // 60) % 24
        window_end_minute = end_minutes % 60

        # Create datetime objects (handle day rollover if needed)
        start_day = report_day_ist
        end_day = report_day_ist

        # If end time is next day
        if end_minutes >= 24 * 60:
            end_day = report_day_ist + timedelta(days=1)

        window_start = ist_tz.localize(datetime.combine(start_day, time(hour=window_start_hour, minute=window_start_minute)))
        window_end = ist_tz.localize(datetime.combine(end_day, time(hour=window_end_hour, minute=window_end_minute)))

        # Create window label
        start_time_str = f"{window_start_hour:02d}:{window_start_minute:02d}"
        end_time_str = f"{window_end_hour:02d}:{window_end_minute:02d}"
        window_label = f"{start_time_str}-{end_time_str}"

        windows.append((window_label, window_start, window_end))

        # Move to next window
        current_minutes += granularity

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

            # Deleted Messages
            try:
                del_resp = cloudwatch.get_metric_statistics(
                    Namespace='AWS/SQS',
                    MetricName='NumberOfMessagesDeleted',
                    Dimensions=[{'Name': 'QueueName', 'Value': queue}],
                    StartTime=s_utc, EndTime=e_utc, Period=300, Statistics=['Sum']
                )
                row[f'Deleted_{win_label}'] = int(sum([dp['Sum'] for dp in del_resp.get('Datapoints', [])]))
            except Exception as e:
                row[f'Deleted_{win_label}'] = f"Error: {str(e)}"

        results.append(row)
    return results


# --- HTML output ---
def write_html(results_by_day, start_date, end_date, start_time, end_time, granularity):
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
        /* Different colors for different metric types */
        th[data-metric="visible"] {{
            background-color: #28a745 !important; /* Green for Visible */
        }}
        th[data-metric="received"] {{
            background-color: #17a2b8 !important; /* Teal for Received */
        }}
        th[data-metric="deleted"] {{
            background-color: #dc3545 !important; /* Red for Deleted */
        }}
        /* Time interval grouping - thinner borders */
        th[data-window-start="true"] {{
            border-left: 2px solid #343a40 !important; /* Thinner dark border to separate time windows */
        }}
        td[data-window-start="true"] {{
            border-left: 2px solid #343a40 !important; /* Thinner dark border for data cells too */
        }}
        /* Subtle borders within the same time window */
        th[data-metric="received"], th[data-metric="deleted"] {{
            border-left: 1px solid #adb5bd; /* Lighter border between metrics in same window */
        }}
        td.metric-received, td.metric-deleted {{
            border-left: 1px solid #adb5bd; /* Lighter border for data cells */
        }}
        /* Sticky first column for queue names */
        th:first-child, td:first-child {{
            position: sticky;
            left: 0;
            background-color: white;
            z-index: 10;
            box-shadow: 2px 0 5px rgba(0,0,0,0.1);
        }}
        th:first-child {{
            background-color: #007bff !important;
            color: white;
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
        .legend {{
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
            border: 1px solid #dee2e6;
        }}
        .legend-title {{
            font-weight: bold;
            margin-bottom: 10px;
            color: #495057;
        }}
        .legend-item {{
            display: inline-block;
            margin: 5px 10px 5px 0;
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 0.8rem;
            color: white;
            font-weight: bold;
        }}
        .legend-visible {{ background-color: #28a745; }}
        .legend-received {{ background-color: #17a2b8; }}
        .legend-deleted {{ background-color: #dc3545; }}
        .legend-early {{
            border-left: 4px solid #ffc107;
            background-color: #fff3cd;
            color: #856404;
            padding-left: 12px;
        }}
        .legend-late {{
            border-left: 4px solid #fd7e14;
            background-color: #ffeaa7;
            color: #8b4513;
            padding-left: 12px;
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
            <strong>Time Windows:</strong> {granularity}-minute intervals from {start_time[0]:02d}:{start_time[1]:02d} - {end_time[0]:02d}:{end_time[1]:02d} IST<br>
            • Each window shows Visible, Received, and Deleted message counts
        </div>
"""

    for day, results in results_by_day.items():
        df = pd.DataFrame(results)

        # Create column rename mapping for time windows
        rename_mapping = {}
        for col in df.columns:
            if col.startswith('Visible_') and col != 'Queue':
                time_window = col.replace('Visible_', '')
                rename_mapping[col] = f'Vis {time_window}'
            elif col.startswith('Received_') and col != 'Queue':
                time_window = col.replace('Received_', '')
                rename_mapping[col] = f'Rec {time_window}'
            elif col.startswith('Deleted_') and col != 'Queue':
                time_window = col.replace('Deleted_', '')
                rename_mapping[col] = f'Del {time_window}'

        df = df.rename(columns=rename_mapping)

        html_content += f"""
        <h2>📅 {day.strftime('%d-%m-%Y')}</h2>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
"""

        # Add table headers with data attributes for styling
        previous_time_window = None

        for col in df.columns:
            if col == 'Queue':
                html_content += f"                    <th>{col}</th>\n"
            else:
                # Determine metric type and extract time window
                metric_type = ""
                time_window = ""

                if col.startswith('Vis '):
                    metric_type = "visible"
                    time_window = col.replace('Vis ', '')
                elif col.startswith('Rec '):
                    metric_type = "received"
                    time_window = col.replace('Rec ', '')
                elif col.startswith('Del '):
                    metric_type = "deleted"
                    time_window = col.replace('Del ', '')

                # Check if this is the start of a new time window
                is_window_start = (time_window != previous_time_window and metric_type == "visible")

                # Build data attributes
                data_attrs = f'data-metric="{metric_type}"'
                if is_window_start:
                    data_attrs += ' data-window-start="true"'

                html_content += f"                    <th {data_attrs}>{col}</th>\n"

                # Update previous time window for next iteration
                if metric_type == "visible":
                    previous_time_window = time_window

        html_content += """                </tr>
            </thead>
            <tbody>
"""

        # Add table rows
        for _, row in df.iterrows():
            html_content += "                <tr>\n"
            previous_time_window = None

            for col in df.columns:
                value = row[col]
                if col == 'Queue':
                    html_content += f'                    <td class="queue-name">{value}</td>\n'
                elif isinstance(value, str) and value.startswith('Error:'):
                    html_content += f'                    <td class="error">{value}</td>\n'
                else:
                    # Determine metric type and time window for data cells
                    metric_type = ""
                    time_window = ""
                    css_class = "metric-value"
                    data_attrs = ""

                    if col.startswith('Vis '):
                        metric_type = "visible"
                        time_window = col.replace('Vis ', '')
                        css_class = "metric-value metric-visible"
                    elif col.startswith('Rec '):
                        metric_type = "received"
                        time_window = col.replace('Rec ', '')
                        css_class = "metric-value metric-received"
                    elif col.startswith('Del '):
                        metric_type = "deleted"
                        time_window = col.replace('Del ', '')
                        css_class = "metric-value metric-deleted"

                    # Check if this is the start of a new time window
                    is_window_start = (time_window != previous_time_window and metric_type == "visible")
                    if is_window_start:
                        data_attrs = ' data-window-start="true"'

                    html_content += f'                    <td class="{css_class}"{data_attrs}>{value}</td>\n'

                    # Update previous time window for next iteration
                    if metric_type == "visible":
                        previous_time_window = time_window

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


# --- Excel output ---
def write_excel(results_by_day, start_date, end_date, start_time, end_time, granularity):
    """Generate Excel file with the same data as HTML report"""
    if start_date == end_date:
        filename = f"sqs_report_{start_date.strftime('%Y%m%d')}.xlsx"
        title = f"SQS Report for {start_date.strftime('%d-%m-%Y')}"
    else:
        filename = f"sqs_report_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.xlsx"
        title = f"SQS Report from {start_date.strftime('%d-%m-%Y')} to {end_date.strftime('%d-%m-%Y')}"

    # Create Excel writer object
    with pd.ExcelWriter(filename, engine='openpyxl') as writer:

        # Process each day's data
        for day, results in results_by_day.items():
            df = pd.DataFrame(results)

            # Create column rename mapping for Excel (more readable than HTML version)
            rename_mapping = {}
            for col in df.columns:
                if col.startswith('Visible_') and col != 'Queue':
                    time_window = col.replace('Visible_', '')
                    rename_mapping[col] = f'Visible {time_window}'
                elif col.startswith('Received_') and col != 'Queue':
                    time_window = col.replace('Received_', '')
                    rename_mapping[col] = f'Received {time_window}'
                elif col.startswith('Deleted_') and col != 'Queue':
                    time_window = col.replace('Deleted_', '')
                    rename_mapping[col] = f'Deleted {time_window}'

            df = df.rename(columns=rename_mapping)

            # Create sheet name from date
            sheet_name = day.strftime('%d-%m-%Y')

            # Write to Excel sheet
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Get the worksheet to apply formatting
            worksheet = writer.sheets[sheet_name]

            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter

                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass

                # Set column width (with some padding)
                adjusted_width = min(max_length + 2, 20)  # Cap at 20 characters
                worksheet.column_dimensions[column_letter].width = adjusted_width

            # Format header row
            for cell in worksheet[1]:
                cell.font = cell.font.copy(bold=True)

    print(f"\n📊 Excel report saved: {filename}")
    return filename

# --- Main ---
if __name__ == "__main__":
    start_date, end_date, ist_tz, start_time, end_time, granularity = parse_args_ist_date_range()
    queues = get_all_sqs_queues()

    results_by_day = {}
    for d in daterange(start_date, end_date):
        print(f"\n=== Generating report for IST date: {d.strftime('%d-%m-%Y')} ===")
        results = get_sqs_metrics(queues, d, ist_tz, start_time, end_time, granularity)
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

    write_html(results_by_day, start_date, end_date, start_time, end_time, granularity)
    excel_file = write_excel(results_by_day, start_date, end_date, start_time, end_time, granularity)
    print("\nAll done ✅")
