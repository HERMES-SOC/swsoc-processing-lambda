"""
This Module contains the FileProcessor class that will distinguish
the appropriate HERMES intrument library to use when processing
the file based off which bucket the file is located in.
"""

import os
import json
from pathlib import Path
import shutil


from sdc_aws_utils.logging import log, configure_logger
from sdc_aws_utils.config import (
    INSTR_TO_PKG,
    parser as science_filename_parser,
    get_instrument_bucket,
)
from sdc_aws_utils.aws import (
    create_s3_client_session,
    object_exists,
    download_file_from_s3,
    upload_file_to_s3,
    create_s3_file_key,
)

# Configure logger
configure_logger()


def handle_event(event, context) -> dict:
    """
    Handles the event passed to the lambda function to initialize the FileProcessor

    :param event: Event data passed from the lambda trigger
    :type event: dict
    :param context: Lambda context
    :type context: dict
    :return: Returns a 200 (Successful) / 500 (Error) HTTP response
    :rtype: dict
    """
    try:
        environment = os.getenv("LAMBDA_ENVIRONMENT", "DEVELOPMENT")

        # Check if SNS or S3 event
        records = json.loads(event["Records"][0]["Sns"]["Message"])["Records"]

        # Parse message from SNS Notification
        for s3_event in records:
            # Extract needed information from event
            s3_bucket = s3_event["s3"]["bucket"]["name"]
            file_key = s3_event["s3"]["object"]["key"]

            FileProcessor(
                s3_bucket=s3_bucket, file_key=file_key, environment=environment
            )

            return {"statusCode": 200, "body": "File Processed Successfully"}

    except Exception as e:
        log.error({"status": "ERROR", "message": e})

        return {
            "statusCode": 500,
            "body": json.dumps(f"Error Processing File: {e}"),
        }


class FileProcessor:
    """
    The FileProcessor class will then determine which instrument
    library to use to process the file.

    :param s3_bucket: The name of the S3 bucket the file is located in
    :type s3_bucket: str
    :param file_key: The name of the S3 object that is being processed
    :type file_key: str
    :param environment: The environment the FileProcessor is running in
    :type environment: str
    :param dry_run: Whether or not the FileProcessor is performing a dry run
    :type dry_run: bool
    """

    def __init__(
        self, s3_bucket: str, file_key: str, environment: str, dry_run: str = None
    ) -> None:
        # Initialize Class Variables
        self.instrument_bucket_name = s3_bucket

        self.file_key = file_key

        # Variable that determines environment
        self.environment = environment

        # Variable that determines if FileProcessor performs a Dry Run
        self.dry_run = dry_run

        # Process File
        self._process_file()

    def _process_file(self) -> None:
        """
        This method serves as the main entry point for the FileProcessor class.
        It will then determine which instrument library to use to process the file.

        :return: None
        :rtype: None
        """
        log.debug(
            {
                "status": "DEBUG",
                "message": "Processing File",
                "instrument_bucket_name": self.instrument_bucket_name,
                "file_key": self.file_key,
                "environment": self.environment,
                "dry_run": self.dry_run,
            }
        )

        # Parse file key to needed information
        (
            parsed_file_key,
            this_instr,
            destination_bucket,
        ) = self._parse_file(self.file_key, self.environment)

        # Download file from S3 or get local file path
        file_path = self._get_file(
            self.instrument_bucket_name,
            self.file_key,
            parsed_file_key,
            self.dry_run,
        )

        # Calibrate/Process file with Instrument Package
        calibrated_filename = self._calibrate_file(this_instr, file_path, self.dry_run)

        # Push file to S3 Bucket
        self._put_file(
            science_filename_parser,
            destination_bucket,
            calibrated_filename,
            self.dry_run,
        )

    @staticmethod
    def _parse_file(file_key, environment):
        """
        Parses the file key to extract the instrument name, and determines the destination bucket based on the instrument and environment.

        :param file_key: The key of the file in the S3 bucket.
        :type file_key: str
        :param environment: The current running environment (e.g., DEVELOPMENT, PRODUCTION).
        :type environment: str
        :return: A tuple containing parsed file key, instrument name, and destination bucket.
        :rtype: tuple
        """
        # Parse file key to get instrument name
        file_key_array = file_key.split("/")
        parsed_file_key = file_key_array[-1]

        # Parse the science file name
        science_file = science_filename_parser(parsed_file_key)
        this_instr = science_file["instrument"]
        destination_bucket = get_instrument_bucket(this_instr, environment)

        return parsed_file_key, this_instr, destination_bucket

    @staticmethod
    def _calibrate_file(instrument, file_path, dry_run=False):
        """
        Calibrates the file using the appropriate instrument library. This involves dynamic import of the calibration module and processing of the file.

        :param instrument: The name of the instrument used for calibration.
        :type instrument: str
        :param file_path: The path to the file that needs to be calibrated.
        :type file_path: Path
        :param dry_run: Indicates whether the operation is a dry run.
        :type dry_run: bool
        :return: The filename of the calibrated file.
        :rtype: string
        """
        try:
            # Dynamically import instrument package
            instr_pkg = __import__(
                f"{INSTR_TO_PKG[instrument]}.calibration",
                fromlist=["calibration"],
            )
            calibration = getattr(instr_pkg, "calibration")

            # If USE_INSTRUMENT_TEST_DATA is set to True, use test data in package
            if os.getenv("USE_INSTRUMENT_TEST_DATA") == "True":
                log.info("Using test data from instrument package")
                instr_pkg_data = __import__(
                    f"{INSTR_TO_PKG[instrument]}.data",
                    fromlist=["data"],
                )
                # Get all files in test data directory
                test_data_dir = Path(instr_pkg_data.__path__[0])
                test_data_files = list(test_data_dir.glob("**/*"))
                log.info(f"Found {len(test_data_files)} files in test data directory")
                log.info(f"Using {test_data_files} as test data")
                # Get any files ending in .bin or .cdf and calibrate them
                for test_data_file in test_data_files:
                    if test_data_file.suffix in [".bin", ".cdf"]:
                        log.info(f"Calibrating {test_data_file}")
                        # Copy file to /test_data directory using shutil
                        test_data_file_path = Path(test_data_file)
                        file_path = Path(f"/test_data/{test_data_file_path.name}")
                        shutil.copy(test_data_file_path, file_path)
                        # Calibrate file
                        calibrated_filename = calibration.process_file(file_path)[0]
                        # Copy calibrated file to test data directory
                        calibrated_file_path = Path(calibrated_filename)
                        # Return name of calibrated file
                        log.info(f"Calibrated file saved as {calibrated_file_path}")

                        return calibrated_filename

                # If no files ending in .bin or .cdf are found, raise an error
                raise FileNotFoundError(
                    "No files ending in .bin or .cdf found in test data directory"
                )

            # Get name of new file
            new_file_path = calibration.process_file(file_path)[0]
            calibrated_filename = new_file_path.name

            return calibrated_filename

        except ValueError as e:
            log.error(e)

    @staticmethod
    def _get_file(instrument_bucket_name, file_key, parsed_file_key, dry_run=False):
        """
        Downloads the file from the specified S3 bucket, if not in a dry run. If a file path is specified in the environment variables, it uses that instead.

        :param instrument_bucket_name: The name of the S3 bucket where the file is located.
        :type instrument_bucket_name: str
        :param file_key: The key of the file in the S3 bucket.
        :type file_key: str
        :param parsed_file_key: The parsed name of the file.
        :type parsed_file_key: str
        :param dry_run: Indicates whether the operation is a dry run.
        :type dry_run: bool
        :return: The path to the downloaded file or None if in a dry run.
        :rtype: Path or None
        """
        # Download file from instrument bucket if not a dry run or use the specified file path
        if not dry_run:
            # Check if using test data in instrument package
            if os.getenv("USE_INSTRUMENT_TEST_DATA") == "True":
                log.info("Using test data from instrument package")
                return None

            # Check if file path is specified in environment variables
            if os.getenv("SDC_AWS_FILE_PATH"):
                log.info(
                    f"Using file path specified in environment variables {os.getenv('SDC_AWS_FILE_PATH')}"
                )
                file_path = Path(os.getenv("SDC_AWS_FILE_PATH"))
                return file_path

            # Initialize S3 Client
            s3_client = create_s3_client_session()

            # Verify object exists in instrument bucket
            if not (
                object_exists(
                    s3_client=s3_client,
                    bucket=instrument_bucket_name,
                    file_key=file_key,
                )
                or dry_run
            ):
                raise FileNotFoundError(
                    f"File {file_key} does not exist in bucket {instrument_bucket_name}"
                )

            # Download file from S3 bucket if no file path is specified
            file_path = download_file_from_s3(
                s3_client,
                instrument_bucket_name,
                file_key,
                parsed_file_key,
            )

            return file_path
        else:
            log.info("Dry Run - File will not be downloaded")
            return None

    @staticmethod
    def _put_file(
        science_filename_parser, destination_bucket, calibrated_filename, dry_run=False
    ):
        """
        Uploads a file to the specified destination bucket in S3, if not in a dry run. Generates the file key for the new file using the given parser.

        :param science_filename_parser: The parser function to generate a file key.
        :type science_filename_parser: function
        :param destination_bucket: The name of the destination S3 bucket.
        :type destination_bucket: str
        :param calibrated_filename: The pathname of the new file to be uploaded.
        :type calibrated_filename: str
        :param dry_run: Indicates whether the operation is a dry run.
        :type dry_run: bool
        :return: The key of the newly uploaded file.
        :rtype: str
        """
        # Generate file key for new file
        new_file_key = create_s3_file_key(science_filename_parser, calibrated_filename)

        # Upload file to destination bucket if not a dry run
        if dry_run:
            log.info("Dry Run - File will not be uploaded")
            return new_file_key

        if os.getenv("USE_INSTRUMENT_TEST_DATA") == "True":
            log.info("Using test data from instrument package")
            return new_file_key

        if not os.getenv("SDC_AWS_FILE_PATH"):
            # Initialize S3 Client
            s3_client = create_s3_client_session()

            # Verify object does not exist in instrument bucket
            if object_exists(
                s3_client=s3_client,
                bucket=destination_bucket,
                file_key=new_file_key,
            ):
                log.warning(
                    f"File {new_file_key} already exists in bucket {destination_bucket}"
                )
                return new_file_key

            # Upload file to destination bucket
            upload_file_to_s3(
                s3_client=s3_client,
                destination_bucket=destination_bucket,
                filename=calibrated_filename,
                file_key=new_file_key,
            )

        else:
            log.info(
                f"File Processed Locally - File will not be uploaded, available in mounted volume as: {Path(calibrated_filename).as_posix()}"
            )

        return new_file_key
