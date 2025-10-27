import requests
import csv
import json
import boto3
from io import StringIO

# Configure your AWS credentials via environment variables or IAM role in Lambda
s3_client = boto3.client('s3')
BUCKET_NAME = 'your-s3-bucket-name'

def process_and_upload(data_type, dataset_url, s3_key_prefix):
    """
    Fetch data from API, transform if needed, and upload JSON to S3.
    
    :param data_type: 'json' or 'csv'
    :param dataset_url: API endpoint URL
    :param s3_key_prefix: Prefix for S3 object key
    """
    # Fetch data
    response = requests.get(dataset_url)
    response.raise_for_status()

    if data_type == 'json':
        data = response.json()
    elif data_type == 'csv':
        csv_text = response.text
        csv_reader = csv.DictReader(StringIO(csv_text))
        data = [row for row in csv_reader]
    else:
        raise ValueError("Invalid data_type. Use 'json' or 'csv'.")

    # Convert to JSON string
    json_data = json.dumps(data)

    # Upload to S3
    s3_key = f"{s3_key_prefix}.json"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=json_data)
    print(f"Uploaded")

def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    Processes multiple datasets using a loop.
    """
    datasets = [
        {"type": "json", "url": "https://api.fda.gov/drug/event.json?limit=100", "prefix": "fda_drug_event"},
        {"type": "csv", "url": "https://data.cityofnewyork.us/resource/h9gi-nx95.csv", "prefix": "nyc_data"}
    ]

    for ds in datasets:
        process_and_upload(ds["type"], ds["url"], ds["prefix"])

    return {"status": "success", "message": "All datasets processed and uploaded."}