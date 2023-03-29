import boto3
import botocore
import pytest
from pathlib import Path
from moto import mock_s3, mock_timestreamwrite
from hermes_core.util import parse_science_filename
import os
from unittest.mock import MagicMock, patch
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


from lambda_function.file_processor.util import (
    parse_file_key,
    create_s3_client_session,
    create_timestream_client_session,
    create_s3_file_key,
    object_exists,
    download_file_from_s3,
    upload_file_to_s3,
    log_to_timestream,
    get_slack_client,
    send_slack_notification,
)
from lambda_function.file_processor.config import parser


TEST_BUCKET = "test-bucket"

BAD_BUCKET = "bad-bucket"

parser = parse_science_filename


@mock_s3
def test_create_s3_client_session_success():
    # Call the function
    result = create_s3_client_session()

    # Check if the result is a boto3 s3 client
    assert result is not None


@mock_s3
def test_create_s3_client_session_failure(monkeypatch):
    # Mock boto3 client function to raise an exception
    monkeypatch.setattr(boto3, "client", lambda x: 1 / 0)

    # Call the function and expect an exception
    with pytest.raises(Exception):
        create_s3_client_session()


@mock_timestreamwrite
def test_create_timestream_client_session_success():
    # Call the function
    result = create_timestream_client_session()

    # Check if the result is a boto3 timestream client
    assert result is not None


@mock_timestreamwrite
def test_create_timestream_client_session_failure(monkeypatch):
    # Mock boto3 client function to raise an exception
    monkeypatch.setattr(boto3, "client", lambda x, region_name: 1 / 0)

    # Call the function and expect an exception
    with pytest.raises(Exception):
        create_timestream_client_session()


def test_parse_file_key_success():
    file_path = "/test_folder/test-file.txt"
    expected_file_key = "test-file.txt"

    # Call the function
    result = parse_file_key(file_path)

    # Check if the result is the expected file key
    assert result == expected_file_key


def test_parse_file_key_failure():
    file_path = None

    # Call the function and expect an exception
    with pytest.raises(Exception):
        parse_file_key(file_path)


def test_create_s3_file_key():
    """
    Test function that tests if the create_s3_file_key function
    returns the correct file key.
    """

    # Test L0 file
    test_valid_file_key = "hermes_EEA_l0_2022335-200137_v01.bin"

    valid_key = create_s3_file_key(parser, old_file_key=test_valid_file_key)

    assert valid_key == "l0/2022/12/hermes_EEA_l0_2022335-200137_v01.bin"

    # Test CDF file
    test_valid_file_key = "hermes_eea_ql_20230205_000006_v1.0.01.cdf"

    valid_key = create_s3_file_key(parser, old_file_key=test_valid_file_key)

    assert valid_key == "ql/2023/02/hermes_eea_ql_20230205_000006_v1.0.01.cdf"

    def test_parser(filename):
        return {"level": "l0"}

    # Test that the function raises a KeyError if the parser does not return a level
    try:
        test_file_key = create_s3_file_key(
            test_parser, old_file_key=test_valid_file_key
        )
    except KeyError as e:
        assert e is not None

    # Test unvalid file key
    test_invalid_file_key = "hermes_EEA_l0_2022335-200137_v01"

    try:
        create_s3_file_key(parser, old_file_key=test_invalid_file_key)
    except ValueError as e:
        assert e is not None


@mock_s3
def test_object_exists():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=TEST_BUCKET)
    s3_client.put_object(Bucket=TEST_BUCKET, Key="test_key", Body="test_data")

    assert object_exists(s3_client, TEST_BUCKET, "test_key")
    assert not object_exists(s3_client, TEST_BUCKET, "non_existent_key")


@mock_s3
def test_download_file_from_s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=TEST_BUCKET)
    s3_client.put_object(Bucket=TEST_BUCKET, Key="test_key", Body="test_data")

    local_path = download_file_from_s3(
        s3_client, TEST_BUCKET, "test_key", "downloaded_key"
    )
    assert local_path == Path("/tmp/downloaded_key")
    assert local_path.is_file()

    # Try to download a non-existent file
    try:
        download_file_from_s3(
            s3_client, TEST_BUCKET, "non_existent_key", "downloaded_key"
        )
    except botocore.exceptions.ClientError as e:
        assert e is not None


@mock_s3
def test_upload_file_to_s3():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=TEST_BUCKET)

    with open("/tmp/test_upload.txt", "w") as f:
        f.write("test_data")

    local_path = upload_file_to_s3(
        s3_client, "test_upload.txt", TEST_BUCKET, "uploaded_key"
    )
    assert local_path == Path("/tmp/test_upload.txt")
    assert (
        s3_client.get_object(Bucket=TEST_BUCKET, Key="uploaded_key")["Body"]
        .read()
        .decode("utf-8")
        == "test_data"
    )
    # Try to upload a non-existent file
    try:
        upload_file_to_s3(s3_client, "bad_test_upload.txt", TEST_BUCKET, "uploaded_key")
    except FileNotFoundError as e:
        assert e is not None

    # Try to upload a file to a non-existent bucket
    try:
        upload_file_to_s3(s3_client, "test_upload.txt", BAD_BUCKET, "uploaded_key")
    except boto3.exceptions.S3UploadFailedError as e:
        assert e is not None


@mock_timestreamwrite
def test_log_to_timestream():
    timestream_client = boto3.client("timestream-write", region_name="us-east-1")

    # Set up the database and table
    try:
        timestream_client.create_database(DatabaseName="sdc_aws_logs")
    except timestream_client.exceptions.ConflictException:
        pass

    try:
        timestream_client.create_table(
            DatabaseName="sdc_aws_logs", TableName="sdc_aws_s3_bucket_log_table"
        )
    except timestream_client.exceptions.ConflictException:
        pass

    # No need to add assertions since we're only testing if the function can be called without exceptions
    log_to_timestream(
        timestream_client,
        "COPY",
        "test_file.txt",
        "L1/2022/09/test_file.txt",
        TEST_BUCKET,
        TEST_BUCKET,
    )

    # Test without passing a timestream client
    try:
        log_to_timestream()
    except Exception as e:
        assert e is not None

    # Test without passing a timestream client
    try:
        log_to_timestream(
            None,
            "COPY",
            "test_file.txt",
            "L1/2022/09/test_file.txt",
        )
    except Exception as e:
        assert e is not None


def test_get_slack_client_success():
    # Set the SLACK_TOKEN environment variable
    os.environ["SLACK_TOKEN"] = "test-token"

    # Call the function
    result = get_slack_client(None)

    # Check if the result is a WebClient
    assert isinstance(result, WebClient)


def test_get_slack_client_failure():
    # Remove the SLACK_TOKEN environment variable
    os.environ.pop("SLACK_TOKEN", None)

    # Call the function
    result = get_slack_client(None)

    # Check if the result is None
    assert result is None


@patch("slack_sdk.WebClient.chat_postMessage")
def test_send_slack_notification_success(mock_chat_postMessage):
    # Create a MagicMock for the WebClient
    slack_client = MagicMock(spec=WebClient)

    # Call the function
    response = send_slack_notification(
        slack_client, "test-channel", "Test message", "success"
    )

    # Check if chat_postMessage was called
    slack_client.chat_postMessage.assert_called()

    # Check if the response is not None
    assert response is not None


@patch("slack_sdk.WebClient.chat_postMessage")
def test_send_slack_notification_failure(mock_chat_postMessage):
    # Create a MagicMock for the WebClient
    slack_client = MagicMock(spec=WebClient)

    # Set chat_postMessage to raise a SlackApiError
    slack_client.chat_postMessage.side_effect = SlackApiError(
        "Error", {"Error": {"Code": "404"}}
    )

    # Call the function and expect an error log
    response = send_slack_notification(
        slack_client, "test-channel", "Test message", "error", 1, 1
    )

    # Check if chat_postMessage was called
    slack_client.chat_postMessage.assert_called()

    # Check if the response is None
    assert response is None
