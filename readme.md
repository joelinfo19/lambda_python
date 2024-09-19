# AWS Lambda Function for Processing S3 Files and Storing in MongoDB

This repository contains an AWS Lambda function designed to process JSON files uploaded to an S3 bucket. The function calculates a unique hash for each file, checks if the file has already been processed based on the hash, and then stores the JSON data into a MongoDB collection, adding the hash as an attribute within the document.

## Table of Contents
- [Overview](#overview)
- [Requirements](#requirements)
- [Environment Variables](#environment-variables)
- [Function Explanation](#function-explanation)


## Overview

This Lambda function performs the following tasks:
1. **Triggers on S3 File Upload:** The function is triggered when a JSON file is uploaded to an S3 bucket.
2. **Calculates File Hash:** The function calculates a SHA-256 hash of the file to uniquely identify its contents.
3. **Checks for Duplicate Processing:** It checks if a document with the same hash already exists in the MongoDB collection to avoid duplicate processing.
4. **Stores JSON Data in MongoDB:** If the file has not been processed, the JSON data is stored in MongoDB, with the hash added as an attribute within the document.
5. **Avoids Reprocessing:** If the file has already been processed, it logs the event and skips the insertion.

## Requirements

- **AWS Lambda:** To deploy and run the function.
- **Amazon S3:** To store and trigger the function with JSON files.
- **MongoDB:** To store processed data.
- **Python 3.8+**: For the Lambda function runtime.

## Environment Variables

Set the following environment variables in your Lambda function configuration:

- `MONGO_URI`: The connection string for your MongoDB instance.
- `DATABASE`: The name of the MongoDB database.
- `COLLECTION`: The name of the MongoDB collection where data will be stored.

## Function Explanation

### 1. Import Necessary Libraries
```python
import json
import boto3
import os
import hashlib
from pymongo import MongoClient
```

 - **json**: For parsing JSON data.
 - **boto3**: For interacting with AWS services.
 - **os**: For accessing environment variables.
 - **hashlib**: For calculating the SHA-256 hash.
 - **pymongo**: For connecting to MongoDB.

### 2. Calculate SHA-256 Hash
```python
def calculate_file_hash(file_path):
    """Calculates the SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()
```

 - **Purpose**: To calculate a unique hash for the file contents.

### 3. Check for Duplicate Processing
```python
def check_duplicate_hash(hash_value):
    """Checks if the hash already exists in the MongoDB collection."""
    result = collection.find_one({"hash": hash_value})
    return result is not None
```

 - **Purpose**: To check if the file has already been processed based on the hash.

### 4. Lambda Handler Function
```python
def lambda_handler(event, context):
    try:
        # Step 1: Extract bucket name and object key
        bucket = event['detail']['bucket']['name']
        key = event['detail']['object']['key']
        
        # Step 2: Download file from S3
        response = s3.get_object(Bucket=bucket, Key=key)
        local_file_path = f"/tmp/{key}"
        with open(local_file_path, 'wb') as file:
            file.write(response['Body'].read())

        # Step 3: Calculate file hash
        file_hash = calculate_file_hash(local_file_path)

        # Step 4: Connect to MongoDB
        client = MongoClient(os.environ['MONGO_URI'])
        db = client[os.environ['DATABASE']]
        collection = db[os.environ['COLLECTION']]

        # Step 5: Check if file has already been processed
        if file_already_processed(collection, file_hash):
            return {"statusCode": 200, "body": "File has already been processed."}

        # Step 6: Load and process JSON data
        with open(local_file_path, 'r', encoding='utf-8') as file:
            json_data = json.load(file)

        # Step 7: Add file hash to JSON data
        if isinstance(json_data, list):
            for document in json_data:
                document['file_hash'] = file_hash
            collection.insert_many(json_data)
        else:
            json_data['file_hash'] = file_hash
            collection.insert_one(json_data)

        return {"statusCode": 200, "body": "File processed successfully."}
    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
```
 - **Purpose**: To handle the Lambda function logic.
 - **Steps**:
    1. Extract bucket name and object key from the event.
    2. Download the file from S3 to a local path.
    3. Calculate the file hash using the `calculate_file_hash` function.
    4. Connect to MongoDB using the provided connection string.
    5. Check if the file has already been processed based on the hash.
    6. Load and process the JSON data from the file.
    7. Add the file hash to the JSON data and store it in the MongoDB collection.


