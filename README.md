# Data Pipeline Assignment
This project fetches 2 apis one has data in json format and other in csv format
The lambda_function.py file gets the data from apis and store it in the S3 bucket and the data
is in form of json for both as I have transformed the csv data into json format.
And I have created a lambda function for lambda_function.py file.

After the data goes to the S3 bucket we use Snowflake for automated ingestion, storage and transformation.

## APIs Used


## FDA Drug Event API

URL: https://api.fda.gov/drug/event.json?limit=100
Description: Provides adverse drug event reports from the FDA database.
Format: JSON



## NYC Motor Vehicle Collisions API

URL: https://data.cityofnewyork.us/resource/h9gi-nx95.csv
Description: Contains data on motor vehicle collisions in New York City.
Format: CSV

## I have attached following Screenshots.


1. Added the python file to AWS Lambda.
2. Got the json data from the AWS Lambda function and it gets stored to AWS S3 Bucket.
3. Snapshot of VSCode where I have written the python file then created its zip and upladed to Lambda.
4. and 5. Screenshot of the transformed view of both the json files
6. Transformed View
7. and 8. Raw Data

