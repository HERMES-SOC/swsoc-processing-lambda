"""
This Module contains the FileProcessor class that will distinguish
the appropriate HERMES intrument library to use when processing
the file based off which bucket the file is located in.
"""
# import boto3
# import botocore
import os
import json
from pathlib import Path

# from slack_sdk.errors import SlackApiError

from sdc_aws_utils.logging import log, configure_logger
from sdc_aws_utils.config import (
    # TSD_REGION,
    INSTR_TO_PKG,
    parser as science_filename_parser,
    get_instrument_bucket,
)
from sdc_aws_utils.aws import (
    create_s3_client_session,
    # create_timestream_client_session,
    object_exists,
    download_file_from_s3,
    upload_file_to_s3,
    # log_to_timestream,
    create_s3_file_key,
)

# from sdc_aws_utils.slack import get_slack_client, send_pipeline_notification
# from cdftracker.database import create_engine
# from cdftracker.database.tables import create_tables
# from cdftracker.tracker import tracker

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
        try:
            self.instrument_bucket_name = s3_bucket
            log.info(
                "Instrument Bucket Name Parsed Successfully:"
                f"{self.instrument_bucket_name}"
            )

        except KeyError:
            error_message = "KeyError when extracting S3 Bucket Name"
            log.error({"status": "ERROR", "message": error_message})
            raise KeyError(error_message)

        try:
            self.file_key = file_key

            log.info(
                {
                    "status": "INFO",
                    "message": "Incoming Object Name"
                    f"Parsed Successfully: {self.file_key}",
                }
            )

        except KeyError:
            error_message = "KeyError when extracting S3 File Key"
            log.error({"status": "ERROR", "message": error_message})
            raise KeyError(error_message)

        # Variable that determines environment
        self.environment = environment

        # Variable that determines if FileProcessor performs a Dry Run
        self.dry_run = dry_run
        if self.dry_run:
            log.warning("Performing Dry Run - Files will not be copied/removed")

        # Process File
        self._process_file()

    def _process_file(self) -> None:
        """
        This method serves as the main entry point for the FileProcessor class.
        It will then determine which instrument library to use to process the file.

        :return: None
        :rtype: None
        """

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
        calibrated_filename = self._calibrate_file(self, this_instr, file_path)

        # Push file to S3 Bucket
        new_file_key = self._put_file(
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
        :return: A tuple containing parsed file key, science file information, instrument name, and destination bucket.
        :rtype: tuple
        """
        # Parse file key to get instrument name
        file_key_array = file_key.split("/")
        parsed_file_key = file_key_array[-1]
        # Parse the science file name
        science_file = science_filename_parser(parsed_file_key)
        this_instr = science_file["instrument"]
        destination_bucket = get_instrument_bucket(this_instr, environment)

        return parsed_file_key, science_file, this_instr, destination_bucket

    @staticmethod
    def _calibrate_file(self, instrument, file_path):
        """
        Calibrates the file using the appropriate instrument library. This involves dynamic import of the calibration module and processing of the file.

        :param instrument: The name of the instrument used for calibration.
        :type instrument: str
        :param file_path: The path to the file that needs to be calibrated.
        :type file_path: Path
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

            log.info(f"Using {INSTR_TO_PKG[instrument]} module for calibration")
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
        if not dry_run and not os.getenv("SDC_AWS_FILE_PATH"):
            # Initialize S3 Client
            s3_client = create_s3_client_session()

            # Upload file to destination bucket
            upload_file_to_s3(
                s3_client=s3_client,
                destination_bucket=destination_bucket,
                filename=calibrated_filename,
                file_key=new_file_key,
            )

        else:
            log.info("Dry Run - File will not be uploaded")

        return new_file_key

    # @staticmethod
    # def _generate_slack_artifacts(
    #     file_path,
    #     new_file_path,
    #     slack_client,
    #     slack_channel,
    #     science_file,
    #     calibrated_filename,
    # ):
    #     """
    #     Generates and sends Slack notifications for the file processing pipeline. Includes error handling for Slack API interactions.

    #     :param file_path: The original file path.
    #     :type file_path: Path
    #     :param new_file_path: The new file path after processing.
    #     :type new_file_path: Path
    #     :param slack_client: The Slack client for sending notifications.
    #     :type slack_client: SlackClient
    #     :param slack_channel: The Slack channel where notifications will be sent.
    #     :type slack_channel: str
    #     :param science_file: Information about the science file.
    #     :type science_file: dict
    #     :param calibrated_filename: The pathname of the new file.
    #     :type calibrated_filename: str
    #     """
    #     try:
    #         # Initialize the slack client
    #         slack_client = get_slack_client(
    #             slack_token=os.getenv("SDC_AWS_SLACK_TOKEN")
    #         )

    #         # Initialize the slack channel
    #         slack_channel = os.getenv("SDC_AWS_SLACK_CHANNEL")

    #         # Send Slack Notification
    #         send_pipeline_notification(
    #             slack_client=slack_client,
    #             slack_channel=slack_channel,
    #             path=calibrated_filename,
    #             alert_type="processed",
    #         )

    #     except SlackApiError as e:
    #         error_code = int(e.response["Error"]["Code"])
    #         if error_code == 404:
    #             log.error(
    #                 {
    #                     "status": "ERROR",
    #                     "message": "Slack Token is invalid",
    #                 }
    #             )

    #     except Exception as e:
    #         log.error(
    #             {
    #                 "status": "ERROR",
    #                 "message": f"Error when initializing slack client: {e}",
    #             }
    #         )

    # @staticmethod
    # def _generate_timestream_artifacts(
    #     file_key, new_file_key, destination_bucket, environment
    # ):
    #     """
    #     Logs file processing events to Amazon Timestream. Handles the initialization of the Timestream client and logs the necessary information.

    #     :param file_key: The key of the original file.
    #     :type file_key: str
    #     :param new_file_key: The key of the processed file.
    #     :type new_file_key: str
    #     :param destination_bucket: The name of the S3 bucket where the processed file is stored.
    #     :type destination_bucket: str
    #     :param environment: The current running environment.
    #     :type environment: str
    #     """
    #     try:
    #         # Initialize Timestream Client
    #         timestream_client = create_timestream_client_session(TSD_REGION)
    #         # Log to timeseries database
    #         log_to_timestream(
    #             timestream_client=timestream_client,
    #             action_type="PUT",
    #             file_key=file_key,
    #             new_file_key=new_file_key,
    #             source_bucket=destination_bucket,
    #             destination_bucket=destination_bucket,
    #             environment=environment,
    #         )

    #     except botocore.exceptions.ClientError:
    #         log.error(
    #             {
    #                 "status": "ERROR",
    #                 "message": "Timestream Client could not be initialized",
    #             }
    #         )

    # @staticmethod
    # def _generate_cdftracker_artifacts(
    #     science_filename_parser, file_path, new_file_path, science_file
    # ):
    #     """
    #     Tracks processed science product in the CDF Tracker file database. It involves initializing the database engine, setting up database tables, and tracking both the original and processed files.

    #     :param science_filename_parser: The parser function to process file names.
    #     :type science_filename_parser: function
    #     :param file_path: The path of the original file.
    #     :type file_path: Path
    #     :param new_file_path: The path of the processed file.
    #     :type new_file_path: Path
    #     :param science_file: Information about the science file.
    #     :type science_file: dict
    #     """
    #     secret_arn = os.getenv("RDS_SECRET_ARN", None)
    #     if secret_arn:
    #         try:
    #             # Get Database Credentials
    #             session = boto3.session.Session()
    #             client = session.client(service_name="secretsmanager")
    #             response = client.get_secret_value(SecretId=secret_arn)
    #             secret = json.loads(response["SecretString"])
    #             connection_string = (
    #                 f"postgresql://{secret['username']}:{secret['password']}@"
    #                 f"{secret['host']}:{secret['port']}/{secret['dbname']}"
    #             )
    #             # Initialize the database engine
    #             database_engine = create_engine(connection_string)

    #             # Setup the database tables if they do not exist
    #             create_tables(database_engine)

    #             # Set tracker to CDFTracker
    #             cdf_tracker = tracker.CDFTracker(
    #                 database_engine, science_filename_parser
    #             )

    #             # If level is L0 should be tracked in CDF
    #             if science_file["level"] == "l0":
    #                 cdf_tracker.track(file_path)

    #             # Track processed file in CDF
    #             cdf_tracker.track(Path(new_file_path))

    #         except Exception as e:
    #             log.error(
    #                 {
    #                     "status": "ERROR",
    #                     "message": f"Error when initializing database engine: {e}",
    #                 }
    #             )
