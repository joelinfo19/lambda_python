import json
import boto3
import os
import hashlib
import logging
from pymongo import MongoClient
from datetime import datetime

# Read the logging level from the environment variable
log_level_str = os.getenv('LOG_LEVEL', 'INFO').upper()  # Default to 'INFO' if not set

# Map the logging level string to its corresponding value
log_levels = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}

# Set up logging
logger = logging.getLogger()
logger.setLevel(log_levels.get(log_level_str, logging.INFO))  # Use the specified level or default to INFO

# Initialize S3 and SNS clients
s3 = boto3.client('s3')
sns_client = boto3.client('sns')

# Connect to MongoDB
mongo_client = MongoClient(os.getenv('MONGO_URI'))

function_name = os.getenv('AWS_LAMBDA_FUNCTION_NAME')

# generate_lambda_message function
def generate_lambda_message(file_name_no_ext, status, recipient_name, error_details=None, sheet_front=None):
    timestamp = datetime.now().isoformat()

    templates = {
        'success': f'''
        Dear {recipient_name},

        A file has just been uploaded to AWS. Here is the status report:

        ================================
        AWS LAMBDA | {function_name}
        ================================

        File: {file_name_no_ext}
        --------------------------------
        Data Inserted Successfully.

        --------------------------------
        Timestamp: {timestamp}
        ''',
        'error_processing': f'''
        Dear {recipient_name},

        A file has just been uploaded to AWS. Here is the status report:

        ================================
        AWS LAMBDA 2 | INGESTOR
        ================================

        File: {file_name_no_ext}
        --------------------------------
        There was an error processing the file uploaded to the bucket.

        Error Details:
        {error_details}

        --------------------------------
        Timestamp: {timestamp}
        '''
    }

    if status == 'success':
        return templates['success']
    elif status == 'error_processing':
        return templates['error_processing']
    else:
        raise ValueError("Unknown status type provided.")


def send_sns_message(arn, subject, message):
    """Send a message to an SNS topic."""
    response = sns_client.publish(
        TopicArn=arn,
        Subject=subject,
        Message=message
    )
    logger.info("SNS publish response: %s", response)

def calculate_file_hash(file_content):
    """Calculates the SHA-256 hash of file content."""
    sha256_hash = hashlib.sha256()
    sha256_hash.update(file_content)
    return sha256_hash.hexdigest()

def file_already_processed(collection, file_hash):
    """Check if the file hash already exists in any document."""
    return collection.find_one({"file_hash": file_hash}) is not None

def estimate_id_exists(collection, estimate_id):
    """Check if a document with the same estimate_id already exists in the collection."""
    return collection.find_one({"estimate_id": estimate_id}) is not None

def process_and_store_data(arn, file_content, collection_name, db_name,error_message,error_message_hash,success_message,file_name):
    """Process the file content and store the data in the specified MongoDB collection."""
    try:


        data = json.loads(file_content)

        # Calculate file hash
        file_hash = calculate_file_hash(file_content)

        # Get collection reference
        collection = mongo_client[db_name][collection_name]

        # Check for duplicate file hash
        if file_already_processed(collection, file_hash):
            logger.warning(f"File with hash {file_hash} already processed.")
            send_sns_message(arn, "WARNING: File Already Processed No Data Inserted", error_message_hash)
            return {"status": "duplicate", "message": "File already processed"}

        # Check for duplicate estimate_id
        if 'estimate_id' in data and estimate_id_exists(collection, data['estimate_id']):
            logger.warning(f"Document with estimate_id {data['estimate_id']} already exists.")
            send_sns_message(arn, "WARNING: Duplicate estimate_id", error_message)

            return {"status": "duplicate", "message": "Estimate ID already exists"}

        # Add hash and timestamp to data and store it in MongoDB
        if isinstance(data, dict):
            data['file_hash'] = file_hash
            data['__createdOn'] = datetime.now()
            data['__fileName'] = file_name
            collection.insert_one(data)
            send_sns_message(arn, "INFO: Inserted a single document into MongoDB collection.", success_message)

        elif isinstance(data, list):
            for document in data:
                document['file_hash'] = file_hash
                document['__createdOn'] = datetime.now()
                document['__fileName'] = file_name
            collection.insert_many(data)
            send_sns_message(arn, "INFO: Inserted many documents into MongoDB collection.", success_message)


        logger.info(f"Data stored successfully in {db_name}.{collection_name}")
        return {"status": "success", "message": "Data stored successfully"}

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        return {"status": "error", "message": str(e)}

def lambda_handler(event, context):
    try:

        logger.info("Received event: %s", json.dumps(event, indent=2))

        # Get S3 event details from EventBridge event
        detail = event['detail']
        bucket_name = detail['bucket']['name']
        object_key = detail['object']['key']
        file_name_no_ext = os.path.splitext(os.path.basename(object_key))[0]

        # Extract client name from bucket name and format DB name
        client_name = bucket_name.split('-')[0]
        db_name = f"{client_name}-db-transform"
        sns_topic_arn = f"arn:aws:sns:us-east-2:730335662357:{client_name}-sns-alerts"

        # Extract folder name from object key and use as collection name
        folder_name = object_key.split('/')[0]
        collection_name = folder_name

      # Define local file path for temporary storage
        tmp_dir = f"/tmp/{collection_name}"
        tmp_file_path = f"{tmp_dir}/{os.path.basename(object_key)}"

        # Ensure the temporary directory exists
        if not os.path.exists(tmp_dir):
            os.makedirs(tmp_dir)

        # Get the file content from S3 and save it locally
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read()
        success_message = generate_lambda_message(object_key.split('/')[-1], 'success', client_name)

        error_message = generate_lambda_message(object_key.split('/')[-1], 'error_processing', client_name, "Duplicate estimate_id found.")
        error_message_hash = generate_lambda_message(object_key.split('/')[-1], 'error_processing', client_name, "File already processed.")

        # Save file content to a temporary file
        with open(tmp_file_path, 'wb') as tmp_file:
            tmp_file.write(file_content)
        # Process the file content and store in MongoDB
        result = process_and_store_data(sns_topic_arn, file_content, collection_name, db_name, error_message,error_message_hash,success_message,file_name_no_ext)
        # Clean up the temporary file
        if os.path.exists(tmp_file_path):
            os.remove(tmp_file_path)


        return {
            "statusCode": 200,
            "body": json.dumps({"message": result["message"]})
        }

    except Exception as e:
        error_message = generate_lambda_message(object_key.split('/')[-1], 'error_processing', client_name, str(e))
        send_sns_message(sns_topic_arn, "Error: Processing Failed", error_message)

        logger.error(f"Unhandled exception: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(e)})
        }
