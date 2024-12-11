import json
import os
import re
from google.cloud import storage
import httpagentparser

def parse_log(data, context):
    """Triggered by a change to a Cloud Storage bucket."""
    try:
        # Define the source and destination bucket names
        source_bucket_name = data['bucket']  # Automatically derived from event data
        source_file_name = data['name']
        destination_bucket_name = os.getenv('DESTINATION_BUCKET', 'apache-logs-parsed')

        print(f"Processing file: {source_file_name} from bucket: {source_bucket_name}")

        # Initialize GCS client
        storage_client = storage.Client()

        # Fetch the source file from the bucket
        source_bucket = storage_client.bucket(source_bucket_name)
        source_blob = source_bucket.blob(source_file_name)

        # Download log data as text
        log_data = source_blob.download_as_text()

        # Regular expression pattern for parsing log entries
        log_pattern = re.compile(r'(?P<host>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] "(?P<request_line>[^"]+)" (?P<status_code>\d{3}) (?P<response_size>\d+) "(?P<referer>[^"]*)" "(?P<user_agent>[^"]*)"')

        parsed_logs = []

        # Iterate through the log data and parse each line
        for line in log_data.splitlines():
            if not line.strip():  # Skip empty lines
                continue

            print(f"Parsing line: {line}")  # Debugging: Log the current line

            match = log_pattern.match(line)
            if match:
                row = match.groupdict()

                # Split request line (method, url, protocol)
                try:
                    method, url, protocol = row["request_line"].split(" ")
                    row.update({"method": method, "url": url, "protocol": protocol})
                except ValueError:
                    print(f"Error splitting request line: {row['request_line']}")
                    continue

                # Parse user agent details
                user_agent = row.get("user_agent", "")
                user_agent_info = httpagentparser.detect(user_agent)
                row.update(user_agent_info)

                # Rename fields
                rename_map = {
                    "status_code": "status",
                    "host": "remoteHostName",
                    "response_size": "responseBytes",
                    "timestamp": "timestamp",
                    "referer": "referer",
                    "user_agent": "userAgent"
                }
                for old_key, new_key in rename_map.items():
                    if old_key in row:
                        row[new_key] = row.pop(old_key)

                # Append the row to the parsed logs
                parsed_logs.append(row)
            else:
                print(f"Could not parse log entry: {line}")

        # Now upload the parsed logs to the destination bucket
        parsed_file_name = source_file_name.replace('access_log', 'parsed_log')
        destination_bucket = storage_client.bucket(destination_bucket_name)
        parsed_blob = destination_bucket.blob(parsed_file_name)

        # Upload parsed logs to the new bucket
        parsed_blob.upload_from_string(json.dumps(parsed_logs, indent=2))

        print(f'Parsed logs uploaded to {destination_bucket_name}/{parsed_file_name}')

    except Exception as e:
        print(f"Error in processing log file: {e}")
