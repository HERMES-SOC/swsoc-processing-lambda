import os
import botocore
import boto3
from lambda_function import log
from pathlib import Path
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import time
from datetime import datetime
from typing import Callable


# Function to create boto3 s3 client session with credentials with try and except
def create_s3_client_session() -> type:
    """
    Create a boto3 s3 client session.

    :return: The boto3 s3 client session
    :rtype: type
    """
    try:
        s3_client = boto3.client("s3")
        return s3_client
    except Exception as e:
        log.error({"status": "ERROR", "message": e})
        raise e


# Function to create boto3 timestream client session with credentials with try and except
def create_timestream_client_session(region: str = "us-east-1") -> type:
    """
    Create a boto3 timestream client session.

    :return: The boto3 timestream client session
    :rtype: type
    """
    try:
        timestream_client = boto3.client("timestream-write", region_name=region)
        return timestream_client
    except Exception as e:
        log.error({"status": "ERROR", "message": e})
        raise e


def parse_file_key(file_path: str) -> str:
    """
    Parse the file key from the file path.

    :param file_path: The file path
    :type file_path: str
    :return: The file key
    :rtype: str
    """
    try:
        file_key = Path(file_path).name

        return file_key
    except Exception as e:
        log.error({"status": "ERROR", "message": e})
        raise e


def create_s3_file_key(science_file_parser: Callable, old_file_key: str) -> str:
    """
    Generate a full S3 file key in the format:
    {level}/{year}/{month}/{file_key}.

    :param file_key: The name of the file
    :type file_key: str
    :return: The full S3 file key
    :rtype: str
    """
    try:
        science_file = science_file_parser(old_file_key)
        print(science_file)
        reference_timestamp = datetime.strptime(
            science_file["time"].value, "%Y-%m-%dT%H:%M:%S.%f"
        )

        # Get Year from science file 'time' key time object
        year = reference_timestamp.year
        month = reference_timestamp.month
        if month < 10:
            month = f"0{month}"

        new_file_key = f"{science_file['level']}/{year}/{month}/{old_file_key}"

        return new_file_key

    except KeyError as e:
        log.error({"status": "ERROR", "message": e})
        raise e


def object_exists(s3_client: type, bucket: str, file_key: str) -> bool:
    """
    Check if a file exists in the specified bucket.

    :param s3_client: The AWS session
    :type s3_client: str
    :param bucket: The name of the bucket
    :type bucket: str
    :param file_key: The name of the file
    :type file_key: str
    :return: True if the file exists, False if it does not
    :rtype: bool
    """

    try:
        s3_client.head_object(Bucket=bucket, Key=file_key)
        return True
    except botocore.exceptions.ClientError:
        return False


def download_file_from_s3(
    s3_client: type, source_bucket: str, file_key: str, parsed_file_key: str
) -> Path:
    """
    Download a file from an S3 bucket.

    :param s3_client: The AWS session
    :type s3_client: str
    :param source_bucket: The name of the source bucket
    :type source_bucket: str
    :param file_key: The name of the file
    :type file_key: str
    :param parsed_file_key: The parsed name of the file
    :type parsed_file_key: str
    :return: The path to the downloaded file
    :rtype: Path
    """
    try:
        # Initialize S3 Client
        log.info(f"Downloading file {parsed_file_key} from {source_bucket}")
        print(f"Downloading file {file_key} from {source_bucket}")

        # Download file to tmp directory
        s3_client.download_file(source_bucket, file_key, f"/tmp/{parsed_file_key}")

        log.debug(f"File {file_key} Successfully Downloaded")

        print(os.listdir("/tmp"))
        return Path(f"/tmp/{parsed_file_key}")

    except botocore.exceptions.ClientError as e:
        log.error({"status": "ERROR", "message": e})

        raise e


def upload_file_to_s3(
    s3_client: str, filename: str, destination_bucket: str, file_key: str
) -> Path:
    """
    Upload a file to an S3 bucket.

    :param session: The AWS session
    :type session: str
    :param filename: The name of the file
    :type filename: str
    :param destination_bucket: The name of the destination bucket
    :type destination_bucket: str
    :param file_key: The name of the file
    :type file_key: str
    :return: The path to the uploaded file
    :rtype: Path
    """
    try:
        # Initialize S3 Client
        log.info(f"Uploading file {file_key} to {destination_bucket}")

        file_path = f"/tmp/{filename}"

        # Upload file to destination bucket
        s3_client.upload_file(file_path, destination_bucket, file_key)

        log.debug(f"File {file_key} Successfully Uploaded")

        return Path(file_path)

    except boto3.exceptions.S3UploadFailedError as e:
        log.error({"status": "ERROR", "message": e})

        raise e


def log_to_timestream(
    timesteam_client: type,
    action_type: str,
    file_key: str,
    new_file_key: str = None,
    source_bucket: str = None,
    destination_bucket: str = None,
) -> None:
    """
    Log information to Timestream.

    :param session: The AWS session
    :type session: str
    :param action_type: The type of action performed
    :type action_type: str
    :param file_key: The name of the file
    :type file_key: str
    :param new_file_key: The new name of the file
    :type new_file_key: str
    :param source_bucket: The name of the source bucket
    :type source_bucket: str
    :param destination_bucket: The name of the destination bucket
    :type destination_bucket: str
    :return: None
    :rtype: None
    """
    log.debug("Logging to Timestream")
    CURRENT_TIME = str(int(time.time() * 1000))
    try:
        if not source_bucket and not destination_bucket:
            raise ValueError("A Source or Destination Buckets is required")

        # Write to Timestream
        timesteam_client.write_records(
            DatabaseName="sdc_aws_logs",
            TableName="sdc_aws_s3_bucket_log_table",
            Records=[
                {
                    "Time": CURRENT_TIME,
                    "Dimensions": [
                        {"Name": "action_type", "Value": action_type},
                        {
                            "Name": "source_bucket",
                            "Value": source_bucket or "N/A",
                        },
                        {
                            "Name": "destination_bucket",
                            "Value": destination_bucket or "N/A",
                        },
                        {"Name": "file_key", "Value": file_key},
                        {
                            "Name": "new_file_key",
                            "Value": new_file_key or "N/A",
                        },
                    ],
                    "MeasureName": "timestamp",
                    "MeasureValue": str(datetime.utcnow().timestamp()),
                    "MeasureValueType": "DOUBLE",
                },
            ],
        )

        log.debug((f"File {file_key} Successfully Logged to Timestream"))

    except Exception as e:
        log.error({"status": "ERROR", "message": e})

        raise e


def get_slack_client(slack_token: str) -> WebClient:
    """
    Initialize a Slack client using the provided token.

    :param slack_token: The Slack API token
    :type slack_token: str
    :return: The initialized Slack WebClient
    :rtype: WebClient
    """
    try:
        # If the slack token is not set, try to get it from the environment
        if not slack_token:
            slack_token = os.environ.get("SLACK_TOKEN")

        # If the slack token is still not set, return None
        if not slack_token:
            log.error(
                {
                    "status": "ERROR",
                    "message": "Slack Token is not set",
                }
            )
            return None

        # Initialize the slack client
        slack_client = WebClient(token=slack_token)

        return slack_client

    except SlackApiError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            log.error(
                {
                    "status": "ERROR",
                    "message": "Slack Token is invalid",
                }
            )
        else:
            log.error(
                {
                    "status": "ERROR",
                    "message": "Slack API Error",
                }
            )
        return None


def send_slack_notification(
    slack_client: WebClient,
    slack_channel: str,
    slack_message: str,
    alert_type: str = "success",
    slack_max_retries: int = 5,
    slack_retry_delay: int = 5,
) -> bool:
    log.debug(f"Sending Slack Notification to {slack_channel}")
    color = {
        "success": "#2ecc71",
        "error": "#ff0000",
    }
    ct = datetime.now()
    ts = ct.strftime("%y-%m-%d %H:%M:%S")

    for i in range(slack_max_retries):
        try:
            slack_client.chat_postMessage(
                channel=slack_channel,
                text=f"{ts} - {slack_message}",
                attachments=[
                    {
                        "color": color[alert_type],
                        "blocks": [
                            {
                                "type": "section",
                                "text": {
                                    "type": "plain_text",
                                    "text": f"{ts} - {slack_message}",
                                },
                            }
                        ],
                    }
                ],
            )

            log.debug(f"Slack Notification Successfully Sent to {slack_channel}")

            return True

        except SlackApiError as e:
            if (
                i < slack_max_retries - 1
            ):  # If it's not the last attempt, wait and try again
                log.warning(
                    f"Error sending Slack Notification (attempt {i + 1}): {e}."
                    f"Retrying in {slack_retry_delay} seconds..."
                )
                time.sleep(slack_retry_delay)
            else:  # If it's the last attempt, log the error and exit the loop
                log.error(
                    {
                        "status": "ERROR",
                        "message": f"Error sending Slack Notification (attempt {i + 1}): {e}",
                    }
                )
                return None
