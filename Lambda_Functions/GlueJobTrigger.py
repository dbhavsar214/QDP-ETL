import json
import boto3
from urllib.parse import unquote_plus
from datetime import datetime
import random
import string

# Initialize AWS Glue and DynamoDB clients
glue = boto3.client('glue')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('processing-1')  

def generate_reference_id():
    """
    Generate a unique reference ID in the format REF123456.
    The ID consists of 'REF' followed by 6 random digits.
    """
    return 'REF' + ''.join(random.choices(string.digits, k=6))

def lambda_handler(event, context):
    """
    Lambda handler function to process an S3 event, start an AWS Glue job,
    and log the job status in DynamoDB.

    Args:
        event: The event payload, which includes S3 object information.
        context: The Lambda runtime information.
    
    Returns:
        dict: A response with status code and message indicating success or failure.
    """
    try:
        # Extract S3 bucket name and object key from the event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = unquote_plus(event['Records'][0]['s3']['object']['key'])

        # Parse email and filename from the object key
        path_parts = object_key.split('/')
        file_path = path_parts[-1]  # Extract the last part of the path (filename)
        
        # Safely split the file name into email and actual file name
        file_parts = file_path.split('_', 1)
        if len(file_parts) == 2:
            email, filename = file_parts
        else:
            print(f"Error: Unexpected file name format: {file_path}")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'Invalid file name format'}),
            }
        
        # Generate a unique reference ID for this job
        reference_id = generate_reference_id()

    
        # Start the Glue job with appropriate parameters
        job_start_time = datetime.utcnow().isoformat()
        input_path = f"s3://{bucket_name}/{object_key}"  # Input S3 file path
        output_path = "s3://processed-json-files/another-sample.csv/"  # Output S3 file path

        params = {
            'JobName': 'json-to-csv',  # Glue job name
            'Arguments': {
                '--input_path': input_path,
                '--output_path': output_path,
                '--reference_ID': reference_id,
                '--email': email
            }
        }

        glue_response = glue.start_job_run(**params)
        job_id = glue_response['JobRunId']

        # Insert job details and status into DynamoDB
        dynamo_params = {
            'reference_id': reference_id,  # Unique reference ID
            'jobId': job_id,  # Glue job ID
            'job_start_time': job_start_time,  # Job start time
            'job_status': 'RUNNING',  # Initial status
            'file_name': filename,  # Name of the processed file
            'email': email  # User's email extracted from file name
        }

        # Add entry to DynamoDB
        try:
            table.put_item(Item=dynamo_params)  
            print(f"DynamoDB entry inserted successfully: {dynamo_params}")
        except Exception as dynamo_error:
            print(f"Error inserting entry into DynamoDB: {dynamo_error}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'File processed successfully and job started!',
                'glueJobId': job_id,
                'reference_id': reference_id
            }),
        }

    except Exception as error:
        print(f"Error in Lambda function: {error}")
        return {
            'statusCode': 500,
            'body': json.dumps('Error processing file and starting Glue job'),
        }
