# Data Pipeline Assignment
This project fetches 2 apis one has data in json format and other in csv format
The lambda_function.py file gets the data from apis and store it in the S3 bucket and the data
is in form of json for both as I have transformed the csv data into json format.
And I have created a lambda function for lambda_function.py file.

After the data goes to the S3 bucket we use Snowflake for automated ingestion, storage and transformation.

## I have attached following Screenshots.

1. 