import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from urllib.parse import unquote_plus
import boto3

# Initialize the S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda handler function to send an email with a download link when a processed file is uploaded to S3.

    Args:
        event: AWS event payload, typically from an S3 trigger.
        context: Runtime context provided by AWS Lambda.

    Returns:
        dict: A response indicating the success or failure of the operation.
    """
    try:
        # Extract bucket name and object key from the S3 event
        bucket_name = event['Records'][0]['s3']['bucket']['name']
        object_key = unquote_plus(event['Records'][0]['s3']['object']['key'])

        print(f"Bucket Name: {bucket_name}")
        print(f"Decoded Object Key: {object_key}")

        # Generate a presigned URL for the uploaded file
        file_url = generate_presigned_url(bucket_name, object_key)
        print(f"Generated presigned URL: {file_url}")

        # Extract the recipient email from the object key (adjust according to the naming convention)
        email = object_key.split('/')[1]  # Assumes object key format: email/output.csv
        print(f"Email to send: {email}")

        # Send an email to the recipient with the download link
        send_email(email, file_url)

        # Return a success response
        return {
            'statusCode': 200,
            'body': json.dumps('Email sent successfully.')
        }

    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def generate_presigned_url(bucket_name, object_key):
    """
    Generate a presigned URL for accessing the specified S3 object.

    Args:
        bucket_name (str): Name of the S3 bucket.
        object_key (str): Key of the object in the bucket.

    Returns:
        str: A presigned URL valid for 1 hour.
    """
    try:
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=3600  # URL expires in 1 hour
        )
        return url
    except Exception as e:
        print(f"Error generating presigned URL: {e}")
        raise

def send_email(recipient_email, file_url):
    """
    Send an email to the specified recipient with the download link.

    Args:
        recipient_email (str): Email address of the recipient.
        file_url (str): Presigned URL for downloading the processed file.
    """
    # Email sender credentials
    sender_email = "dibhavsar214@gmail.com"  # Replace with your email
    sender_password = "dmmc bqeh zdfw ticx"  # Replace with your app-specific password or email password
    smtp_server = "smtp.gmail.com"  # Gmail SMTP server
    smtp_port = 587  # SMTP port for TLS

    # Email content
    subject = "Your Processed File is Ready"
    body = f"Your processed file is available for download at the following link:\n\n{file_url}"

    # Construct the email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = recipient_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        # Establish connection to the SMTP server and send the email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Secure the connection using TLS
            server.login(sender_email, sender_password)  # Login to the SMTP server
            server.sendmail(sender_email, recipient_email, msg.as_string())  # Send the email
            print(f"Email sent successfully to {recipient_email}")
    except Exception as e:
        print(f"Error sending email: {e}")
        raise
