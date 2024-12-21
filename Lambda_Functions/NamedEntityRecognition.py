import json
import boto3
import spacy
import en_core_web_sm
import base64
from datetime import datetime
import random
import string
from urllib.parse import parse_qs
import cgi

# Initialize spaCy model for Named Entity Recognition (NER)
nlp = en_core_web_sm.load()

# Initialize DynamoDB resource and reference the table
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('processing-1')

# Function to generate a reference ID for each request
def generate_reference_id():
    return 'REF' + ''.join(random.choices(string.digits, k=6))  # Generates a reference ID like REF123456

def lambda_handler(event, context):
    """
    Lambda function to process a form submission containing a text file, 
    perform Named Entity Recognition (NER) using spaCy, and store the results 
    in DynamoDB. It handles multipart form-data requests with Base64-encoded file content.

    Args:
        event (dict): The event object that contains HTTP request data (form fields and file content).
        context (object): Lambda context object (unused in this function).

    Returns:
        dict: A response containing the status of the operation and the identified entities (if successful).
    """
    
    # Debugging: Print the headers and body of the incoming request
    print("Headers:", event.get('headers'))
    print("Body:", event.get('body'))
    
    try:
        print("Received event:", json.dumps(event))  # Debugging: Log the full event structure

        # Retrieve content type to ensure the request is multipart/form-data
        content_type = event['headers'].get('Content-Type') or event['headers'].get('content-type')
        if not content_type.startswith('multipart/form-data'):
            raise Exception('Invalid content type: Expected multipart/form-data')

        # Parse the multipart/form-data body using cgi.FieldStorage
        form_data = cgi.FieldStorage(
            fp=event['body'].encode('utf-8') if event.get("isBase64Encoded") else event['body'], 
            environ={
                'REQUEST_METHOD': 'POST',
                'CONTENT_TYPE': content_type,
            }
        )

        # Extract required form fields: email, file name, and base64-encoded file content
        email = form_data.getvalue('email')
        file_name = form_data.getvalue('file_name')
        file_content_base64 = form_data.getvalue('file_content')

        # Validate that all required fields are present
        if not email or not file_name or not file_content_base64:
            raise Exception("Missing required form fields: 'email', 'file_name', or 'file_content'")

        # Decode the Base64-encoded file content to a string
        file_content = base64.b64decode(file_content_base64).decode('utf-8')

        # Generate a unique reference ID for the job
        reference_id = generate_reference_id()

        # Capture the job start time (UTC)
        job_start_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        # Process the file content with spaCy for Named Entity Recognition (NER)
        doc = nlp(file_content)
        entities = [(ent.text, ent.label_) for ent in doc.ents]  # Extract entities as tuples of (text, label)

        # Store job information and metadata in DynamoDB
        dynamo_item = {
            'reference_id': reference_id,  # Unique job reference ID
            'email': email,  # User's email from the form
            'file_name': file_name,  # File name from the form
            'job_start_time': job_start_time,  # Start time of the job
            'job_end_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),  # End time of the job (UTC)
            'job_status': 'SUCCESS',  # Status of the job (can be SUCCESS or ERROR)
            'job_type': 'NER'  # Type of the job (Named Entity Recognition)
        }
        table.put_item(Item=dynamo_item)  # Insert job metadata into DynamoDB

        # Return a success response with the identified entities
        return {
            'statusCode': 200,  # HTTP status code for success
            'headers': {
                "Access-Control-Allow-Origin": "*",  # Allow any origin to access this Lambda
                "Access-Control-Allow-Methods": "POST, GET, OPTIONS",  # Allowed HTTP methods
            },
            'body': json.dumps({
                'status': 'SUCCESS',  # Success status
                'entities': entities  # List of entities identified by spaCy NER
            })
        }

    except Exception as e:
        print(f"Error processing file: {str(e)}")  # Log the error for debugging purposes
        # Return an error response with a message describing the issue
        return {
            'statusCode': 500,  # HTTP status code for server error
            'headers': {
                "Access-Control-Allow-Origin": "*",  # Allow any origin to access this Lambda
                "Access-Control-Allow-Methods": "POST, GET, OPTIONS",  # Allowed HTTP methods
            },
            'body': json.dumps({
                'status': 'ERROR',  # Error status
                'message': str(e)  # Error message explaining what went wrong
            })
        }
