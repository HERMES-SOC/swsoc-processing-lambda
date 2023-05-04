"""
Module which contains the handler function and the main function
which contains the logicthat initializes the FileProcessor class
in it's correct environment.
"""

import json
import logging

from lambda_function.file_processor.file_processor import FileProcessor  # noqa: E402


# Set up logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# To remove boto3 noisy debug logging
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)


def handler_function(event, context) -> dict:
    """
    Lambda handler function that passes variables to the function that
    handles the logic that initializes the FileProcessor class in it's correct
    environment.

    :param event: Event data passed from the lambda trigger
    :type event: dict
    :param context: Lambda context
    :type context: dict
    :return: Returns a 200 (Successful) / 500 (Error) HTTP response
    :rtype: dict
    """
    # Extract needed information from event
    try:
        # Check if SNS or S3 event and parse accordingly

        # Parse SNS event
        records = json.loads(event["Records"][0]["Sns"]["Message"])["Records"]

        # Parse message from SNS Notification
        for s3_event in records:
            # Extract needed information from event
            s3_bucket = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]

            FileProcessor(s3_bucket=s3_bucket, file_key=file_key)

        return {
            "statusCode": 200,
            "body": json.dumps("File Processed Successfully"),
        }

    except Exception as e:
        # Pass required variables to sort function and returns a 200 (Successful)
        # / 500 (Error) HTTP response
        log.error({"status": "ERROR", "message": e})

        return {
            "statusCode": 500,
            "body": json.dumps(f"Error Processing File: {e}"),
        }
