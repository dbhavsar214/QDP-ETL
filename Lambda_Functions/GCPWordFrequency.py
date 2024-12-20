from google.cloud import firestore, storage, bigquery
import json
import uuid
import re
import collections
from datetime import datetime
import functions_framework
from flask import make_response

# Initialize clients for Firestore, Google Cloud Storage, and BigQuery
db = firestore.Client()
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Define constants for the resources used
bucket_name = 'dp3--txt-file-storage'  # Cloud Storage bucket for file storage
dataset_name = 'process_3'  # BigQuery dataset
table_name = 'data_3'  # BigQuery table for word frequency data

@functions_framework.http
def store_file(request):
    """
    Cloud Function to process an uploaded file, store metadata in Firestore,
    save file content in Cloud Storage, and insert word frequency data into BigQuery.

    Args:
        request (flask.Request): The HTTP request object.

    Returns:
        Response: A JSON response with status and message.
    """
    # Initialize the response object to return
    response = make_response()

    # Handle preflight requests (CORS) for allowing cross-origin resource sharing
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Origin'] = '*'  # Allow any origin or specify specific origins
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'  # Allowed methods
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'  # Allowed headers
        return response

    try:
        # Parse the incoming JSON data from the request
        data = request.get_json()

        # Validate that the required fields are present in the incoming data
        required_fields = ['fileName', 'email', 'fileContent']
        if not all(field in data for field in required_fields):
            return json.dumps({"error": "Missing required fields"}), 400

        # Extract the fields from the parsed JSON data
        file_name = data['fileName']
        email = data['email']
        file_content = data['fileContent']
        reference_id = data['referenceId']

        # Generate a unique identifier for this file processing (UUID)
        unique_id = uuid.uuid4().hex
        temp_id = unique_id + file_name  # Temporary ID that combines UUID with file name for uniqueness

        # Save metadata about the file to Firestore
        doc_ref = db.collection("file_metadata").document(unique_id)
        doc_ref.set({
            "id": unique_id,
            "fileName": file_name,
            "email": email,
            "uploadedAt": firestore.SERVER_TIMESTAMP,
            "location": f"gs://{bucket_name}/uploads/{temp_id}",
            "status": "Processing",  # Set initial status as 'Processing'
            "referenceId": reference_id
        })

        # Store the file content in Google Cloud Storage
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"uploads/{temp_id}")
        blob.upload_from_string(file_content, content_type="text/plain")

        # Compute word frequencies from the file content (text processing)
        words = re.findall(r'\b\w+\b', file_content.lower())  # Find all words (case-insensitive)
        word_count = collections.Counter(words)  # Count the frequency of each word

        # Prepare data for BigQuery (each word and its frequency)
        rows_to_insert = [
            {"document_id": unique_id, "word": word, "frequency": count, "file_name": file_name, "email": email}
            for word, count in word_count.items()
        ]

        # BigQuery table ID, including project, dataset, and table name
        table_id = f"{bigquery_client.project}.{dataset_name}.{table_name}"

        # Insert each word-frequency pair into BigQuery
        for row in rows_to_insert:
            query = f"""
            INSERT INTO {table_id} (document_id, word, frequency, file_name, email)
            VALUES ("{row['document_id']}", "{row['word']}", {row['frequency']}, "{row['file_name']}", "{row['email']}")
            """
            bigquery_client.query(query).result()  # Run the query and wait for it to complete

        # Update the Firestore document status to 'Ready' after successful processing
        doc_ref.update({
            "status": "Ready for Looker Studio",  # Indicate that the file is ready for further use
            "processedAt": firestore.SERVER_TIMESTAMP
        })

        # Set CORS headers for the response to allow client-side JavaScript to access the response
        response.headers['Access-Control-Allow-Origin'] = '*'  # Allow any origin or specify a specific origin

        # Return a success response with a message and the file URL
        response.data = json.dumps({
            "message": "File successfully processed and data inserted into BigQuery.",
            "recordId": unique_id,  # Unique identifier for the processing
            "fileUrl": f"gs://{bucket_name}/uploads/{temp_id}",  # URL to access the uploaded file in Cloud Storage
        })
        response.status_code = 200

    except Exception as e:
        # If an error occurs, update Firestore document status to 'Failed' and log the error message
        if 'doc_ref' in locals():
            doc_ref.update({
                "status": "Failed",  # Indicate that the processing failed
                "errorMessage": str(e)  # Store the error message
            })

        # Return an error response with the error message
        response.data = json.dumps({"error": str(e)})
        response.status_code = 500

    return response
