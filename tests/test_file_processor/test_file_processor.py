import os
import boto3
import pytest
from moto import mock_s3, mock_timestreamwrite
from lambda_function.file_processor.file_processor import FileProcessor

# Constants for testing
DB_HOST = "sqlite:////tmp/test.db"

TEST_BUCKET = "hermes-spani"
TEST_BAD_FILE = "./tests/test_files/test-file-key.txt"
TEST_L0_FILE = "./tests/test_files/hermes_SPANI_l0_2023040-000018_v01.bin"
TEST_QL_FILE = "./tests/test_files/hermes_spn_ql_20230210_000018_v1.0.01.cdf"
TEST_L1_FILE = "./tests/test_files/hermes_spn_l1_20230210_000018_v1.0.01.cdf"
TEST_REGION = "us-east-1"


TEST_FILES = [TEST_L0_FILE, TEST_L1_FILE, TEST_QL_FILE]


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_s3():
        conn = boto3.client("s3", region_name="us-west-2")
        conn.create_bucket(
            Bucket=TEST_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "us-west-2"},
        )
        yield conn


@pytest.fixture(scope="function")
def timestream_client(aws_credentials):
    with mock_timestreamwrite():
        conn = boto3.client("timestream-write", region_name=TEST_REGION)

        yield conn


def test_file_processor(s3_client, timestream_client):
    for file in TEST_FILES:
        # # Upload test file to mock S3
        # for file in TEST_FILES:
        s3_client.put_object(Bucket=TEST_BUCKET, Key=file, Body="Sample data")

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

        # Test FileProcessor with
        file_processor = FileProcessor(
            TEST_BUCKET,
            file,
            dry_run=False,
            timestream_client=timestream_client,
            s3_client=s3_client,
            db_host=DB_HOST,
            slack_token="test-token",
            slack_channel="test-channel",
            slack_retries=0,
            slack_retry_delay=0,
        )

        assert file_processor is not None

    # Test FileProcessor with slack token
