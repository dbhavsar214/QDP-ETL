import sys
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import *
import datetime

# Create a Spark session for distributed data processing
spark = SparkSession.builder.getOrCreate()

# Initialize DynamoDB resource to interact with a particular table
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('processing-1')  


def read_nested_json(df):
    """
    Flatten one level of nested JSON structure.
    
    Parameters:
        df (DataFrame): Input Spark DataFrame with potentially nested JSON data.
    
    Returns:
        DataFrame: Flattened DataFrame after processing arrays and structs.
    """
    column_list = []
    for column_name in df.schema.names:
        # Check if the column is an array; if so, explode it
        if isinstance(df.schema[column_name].dataType, ArrayType):
            df = df.withColumn(column_name, explode(column_name))
            column_list.append(column_name)
        # Check if the column is a struct; if so, flatten it into subfields
        elif isinstance(df.schema[column_name].dataType, StructType):
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        # Retain all other columns as is
        else:
            column_list.append(column_name)
    # Return the modified DataFrame with selected columns
    df = df.select(column_list)
    return df


def flatten(df):
    """
    Recursively flatten a nested JSON structure until no more nested fields remain.
    
    Parameters:
        df (DataFrame): Input Spark DataFrame with nested JSON data.
    
    Returns:
        DataFrame: Fully flattened DataFrame.
    """
    read_nested_json_flag = True
    while read_nested_json_flag:
        # Apply the flattening logic
        df = read_nested_json(df)
        read_nested_json_flag = False
        # Check if further flattening is required by inspecting the schema
        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True
    return df


def update_dynamodb_status(reference_id, status, s3_object_key=None):
    """
    Update the status of a job in DynamoDB, optionally adding an S3 object key.
    
    Parameters:
        reference_id (str): The unique reference ID for the job.
        status (str): Job status (e.g., 'SUCCEEDED', 'FAILED').
        s3_object_key (str, optional): The S3 key of the output file.
    
    Returns:
        None
    """
    try:
        # Record the current UTC time for the job's end timestamp
        job_end_time = datetime.datetime.utcnow().isoformat()

        # Clean and format the S3 object key, if provided
        if s3_object_key:
            s3_object_key = s3_object_key.lstrip('s3:/').rstrip('/').replace('//', '/')

        # Define the update expression for DynamoDB
        update_expression = "set job_status = :status, job_end_time = :end_time"
        expression_values = {
            ':status': status,
            ':end_time': job_end_time
        }

        # Add S3 object key to the update expression if present
        if s3_object_key:
            update_expression += ", s3_object_key = :s3_key"
            expression_values[':s3_key'] = s3_object_key

        # Update the item in DynamoDB
        response = table.update_item(
            Key={'reference_id': reference_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values
        )

        print(f"DynamoDB updated successfully: Reference ID {reference_id}, Status {status}, S3 Key {s3_object_key}")
    
    except NoCredentialsError:
        print("No AWS credentials found.")
    except PartialCredentialsError:
        print("Incomplete AWS credentials configuration.")
    except Exception as e:
        print(f"Error updating DynamoDB: {e}")


def main():
    """
    Main function to process a JSON file:
    - Reads a nested JSON file from S3.
    - Flattens the JSON data.
    - Saves the flattened data back to S3 as a CSV.
    - Updates the job status in DynamoDB.
    """
    # Parse input arguments
    args = getResolvedOptions(sys.argv, ['input_path', 'output_path', 'reference_ID', 'email'])
    input_file_path = args.get('input_path', '').strip()
    output_path = args.get('output_path', '').strip()
    reference_ID = args.get('reference_ID')
    email = args.get('email')
    print(f"Email extracted from arguments: {email}")

    # Validate input arguments
    if not input_file_path:
        raise ValueError("The 'input_path' argument is missing or empty.")
    if not output_path:
        raise ValueError("The 'output_path' argument is missing or empty.")

    print(f"Input file path: {input_file_path}")
    print(f"Output path: {output_path}")
    print(f"Reference ID: {reference_ID}")

    try:
        # Read the JSON file from S3
        df = spark.read.option("multiline", True).json(input_file_path)
        # Flatten the JSON structure
        df_flattened = flatten(df)

        # Print a preview of the flattened DataFrame for debugging
        print("Preview of flattened DataFrame:")
        df_flattened.show(5)

        # Generate a unique output path using the current timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_output_path = f"{output_path}/{email}/{reference_ID}_output_{timestamp}.csv"

        # Save the flattened DataFrame as a CSV file in S3
        df_flattened.coalesce(1).write.format("csv").option("header", "true").save(unique_output_path)

        # Update the DynamoDB status to indicate success
        update_dynamodb_status(reference_ID, "SUCCEEDED", unique_output_path)

    except Exception as e:
        # Log the error and update the DynamoDB status to indicate failure
        print(f"Error processing file: {e}")
        update_dynamodb_status(reference_ID, "FAILED")
        raise  # Re-raise the exception to mark the Glue job as failed


# Entry point for the script
if __name__ == "__main__":
    main()
