import os
import json
import boto3
import pytest
from moto import mock_s3, mock_sns

# Set up environment variables
os.environ["LAMBDA_ENVIRONMENT"] = "PRODUCTION"

from lambda_function.handler import handler_function  # noqa: E402


@pytest.fixture
def sns_event():
    return {
        "Records": [
            {
                "Sns": {
                    "Message": json.dumps(
                        {
                            "Records": [
                                {
                                    "s3": {
                                        "bucket": {"name": "hermes-merit"},
                                        "object": {
                                            "key": "l0/2023/02/hermes_MERIT_l0_2023037-000019_v01.bin"
                                        },
                                    }
                                }
                            ]
                        }
                    )
                }
            }
        ]
    }


@pytest.fixture
def bad_sns_event():
    return {"Records": []}


@mock_s3
@mock_sns
def test_handler_success(sns_event):
    # Set up S3 bucket and object
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")
    s3.put_object(Bucket="test-bucket", Key="test-file.txt", Body="Test content")

    # Call the handler function
    result = handler_function(sns_event, {})

    # Check if the result is successful
    assert result["statusCode"] == 200
    assert json.loads(result["body"]) == "File Processed Successfully"


@mock_s3
@mock_sns
def test_handler_failure(bad_sns_event):
    # Set up S3 bucket without object
    s3 = boto3.client("s3")
    s3.create_bucket(Bucket="test-bucket")

    # Unset the environment variable
    os.environ.pop("LAMBDA_ENVIRONMENT", None)

    # Call the handler function
    result = handler_function(bad_sns_event, {})

    # Check if the result is an error
    assert result["statusCode"] == 500
    assert "Error Processing File" in json.loads(result["body"])
