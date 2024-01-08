import pytest
import os
import boto3
import json
from moto import mock_s3
from pathlib import Path
from botocore.exceptions import ClientError


os.environ["SDC_AWS_CONFIG_FILE_PATH"] = "lambda_function/src/config.yaml"
from src.file_processor.file_processor import (
    handle_event,
    FileProcessor,
)
from sdc_aws_utils.config import (
    parser as science_filename_parser,
)


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture
def s3(aws_credentials):
    with mock_s3():
        yield


def test_file_key_generation():
    # Setup
    filename = "hermes_EEA_l0_2023042-000000_v0.bin"

    # Exercise
    file_key = FileProcessor._put_file(
        science_filename_parser, "hermes-eea", filename, True
    )

    # Verify
    assert file_key == "l0/2023/02/hermes_EEA_l0_2023042-000000_v0.bin"


@pytest.mark.parametrize("dry_run", [False, True])
def test_s3_upload(s3, dry_run):
    # Setup
    bucket = "hermes-eea"
    filename = "hermes_EEA_l0_2023042-000000_v0.bin"
    expected_key = "l0/2023/02/hermes_EEA_l0_2023042-000000_v0.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)

    # Create file in tmp directory to fix /tmp/hermes_EEA_l0_2023042-000000_v0.bin'
    with open(f"/tmp/{filename}", "w") as f:
        f.write("test")

    # Exercise
    file_key = FileProcessor._put_file(
        science_filename_parser, bucket, filename, dry_run
    )

    # Verify
    assert file_key == expected_key
    if not dry_run:
        assert s3_client.head_object(Bucket=bucket, Key=expected_key)
    else:
        with pytest.raises(ClientError):
            s3_client.head_object(Bucket=bucket, Key=expected_key)

    # Cleanup
    s3_client.delete_object(Bucket=bucket, Key=expected_key)
    s3_client.delete_bucket(Bucket=bucket)
    os.remove(f"/tmp/{filename}")


@pytest.mark.parametrize("dry_run", [False, True])
def test_s3_upload(s3, dry_run):
    # Setup
    bucket = "hermes-eea"
    filename = "hermes_EEA_l0_2023042-000000_v0.bin"
    expected_key = "l0/2023/02/hermes_EEA_l0_2023042-000000_v0.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)

    # Create file in tmp directory to fix /tmp/hermes_EEA_l0_2023042-000000_v0.bin'
    with open(f"/tmp/{filename}", "w") as f:
        f.write("test")

    # Exercise
    file_key = FileProcessor._put_file(
        science_filename_parser, bucket, filename, dry_run
    )

    # Verify
    assert file_key == expected_key
    if not dry_run:
        assert s3_client.head_object(Bucket=bucket, Key=expected_key)
        try:
            file_key = FileProcessor._put_file(
                science_filename_parser, bucket, filename, dry_run
            )
        except Exception as e:
            assert e is not None
    else:
        with pytest.raises(ClientError):
            s3_client.head_object(Bucket=bucket, Key=expected_key)


def test_with_sdc_aws_file_path_set():
    # Setup
    parser_mock = lambda filename: filename
    bucket = "hermes-eea"
    filename = "hermes_EEA_l0_2023042-000000_v0.bin"
    expected_key = "l0/2023/02/hermes_EEA_l0_2023042-000000_v0.bin"

    os.environ["SDC_AWS_FILE_PATH"] = f"../test_data/{filename}"

    # Exercise
    file_key = FileProcessor._put_file(science_filename_parser, bucket, filename, False)

    # Cleanup
    del os.environ["SDC_AWS_FILE_PATH"]

    # Verify
    assert file_key == expected_key


def test_file_download_from_s3(s3):
    # Setup
    bucket = "hermes-eea"
    file_key = "l0/2023/02/hermes_EEA_l0_2023042-000000_v0.bin"
    parsed_file_key = "hermes_EEA_l0_2023042-000000_v0.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=file_key, Body="test content")

    # Exercise
    file_path = FileProcessor._get_file(bucket, file_key, parsed_file_key)

    # Verify
    assert file_path is not None
    assert file_path.name == parsed_file_key

    # Cleanup
    os.remove(file_path)


def test_file_download_with_env_var_set():
    # Setup
    os.environ["SDC_AWS_FILE_PATH"] = "/path/to/test-file.bin"

    # Exercise
    file_path = FileProcessor._get_file("bucket", "file_key", "parsed_file_key")

    # Verify
    assert file_path == Path("/path/to/test-file.bin")

    # Cleanup
    del os.environ["SDC_AWS_FILE_PATH"]


@pytest.mark.parametrize("dry_run", [True, False])
def test_dry_run_behavior(s3, dry_run):
    # Setup
    bucket = "hermes-eea"
    file_key = "l0/2023/02/hermes_EEA_l0_2023042-000000_v0.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=file_key, Body="test content")

    # Exercise
    file_path = FileProcessor._get_file(
        bucket, file_key, "hermes_EEA_l0_2023042-000000_v0.bin", dry_run
    )

    # Verify
    if dry_run:
        assert file_path is None
    else:
        assert file_path is not None
        assert file_path.name == "hermes_EEA_l0_2023042-000000_v0.bin"
        assert file_path.parent == Path("/tmp")


def test_file_not_found_in_s3_bucket(s3):
    # Setup
    bucket = "test-bucket"
    file_key = "nonexistent-file.bin"
    parsed_file_key = "parsed-nonexistent-file.bin"

    # Setup S3
    s3_client = boto3.client("s3")
    s3_client.create_bucket(Bucket=bucket)

    # Exercise and Verify
    with pytest.raises(FileNotFoundError):
        FileProcessor._get_file(bucket, file_key, parsed_file_key)


def test_file_calibrate():
    # Setup
    intrument = "eea"
    file_path = "lambda_function/tests/test_data/hermes_EEA_l0_2023042-000000_v0.bin"

    # Calibrate
    file_path = Path(file_path)

    calibrated_file_path = FileProcessor._calibrate_file(intrument, file_path)

    # Verify
    assert calibrated_file_path == "hermes_eea_l1_20230211T000000_v1.0.0.cdf"


def test_file_calibrate_failure():
    # Setup
    intrument = "eea"
    file_path = "lambda_function/tests/test_data/nonexistent-file.bin"

    # Calibrate
    file_path = Path(file_path)

    calibrated_file_path = FileProcessor._calibrate_file(intrument, file_path)

    # Verify
    assert calibrated_file_path is None


def test_file_parse_prod():
    # Setup
    file_key = "test/path/hermes_EEA_l0_2023042-000000_v0.bin"
    environment = "PRODUCTION"

    parsed_file_key, this_instr, destination_bucket = FileProcessor._parse_file(
        file_key, environment
    )

    # Verify
    assert parsed_file_key == "hermes_EEA_l0_2023042-000000_v0.bin"

    # assert this_instr is a string and is equal to 'eea'
    assert isinstance(this_instr, str)
    assert this_instr == "eea"

    # assert destination_bucket is a string and is equal to 'hermes-eea'
    assert isinstance(destination_bucket, str)
    assert destination_bucket == "hermes-eea"


def test_file_parse_dev():
    # Setup
    file_key = "test/path/hermes_EEA_l0_2023042-000000_v0.bin"
    environment = "DEVELOPMENT"

    parsed_file_key, this_instr, destination_bucket = FileProcessor._parse_file(
        file_key, environment
    )

    # Verify
    assert parsed_file_key == "hermes_EEA_l0_2023042-000000_v0.bin"

    # assert this_instr is a string and is equal to 'eea'
    assert isinstance(this_instr, str)
    assert this_instr == "eea"

    # assert destination_bucket is a string and is equal to 'hermes-eea'
    assert isinstance(destination_bucket, str)
    assert destination_bucket == "dev-hermes-eea"


# Test handle event and pass in the test_eea_event.json file as json
def test_handle_event(s3):
    filename = "hermes_EEA_l0_2023042-000000_v0.bin"

    # Set the absolute path using file_path as string
    os.environ["SDC_AWS_FILE_PATH"] = f"lambda_function/tests/test_data/{filename}"

    # Setup
    event = json.load(open("lambda_function/tests/test_data/test_eea_event.json"))

    # Exercise
    response = handle_event(event, None)

    # Verify
    assert response["statusCode"] == 200

    # Test unexpected event
    filename = "nonexistent-file.bin"
    os.environ["SDC_AWS_FILE_PATH"] = f"lambda_function/tests/test_data/{filename}"

    # Setup
    event = json.load(open("lambda_function/tests/test_data/test_eea_event.json"))

    # Exercise
    response = handle_event(event, None)

    # Verify
    assert response["statusCode"] == 500
