"""
This Module contains the FileProcessor class that will distinguish
the appropriate HERMES intrument library to use when processing
the file based off which bucket the file is located in.
"""

import os
import os.path

from cdftracker.database import create_engine
from cdftracker.database.tables import create_tables
from cdftracker.tracker import tracker

from sdc_aws_utils.logging import log
from sdc_aws_utils.aws import (
    create_s3_client_session,
    create_timestream_client_session,
    download_file_from_s3,
    parse_file_key,
    upload_file_to_s3,
    log_to_timestream,
    object_exists,
    create_s3_file_key,
)
from sdc_aws_utils.slack import get_slack_client, send_slack_notification
from sdc_aws_utils.config import (
    parser,
    INSTR_TO_BUCKET_NAME,
    INSTR_TO_PKG,
)


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
    :param s3_client: The boto3 s3 client to use
    :type s3_client: boto3.client
    :param timestream_client: The boto3 timestream client to use
    :type timestream_client: boto3.client
    :param db_host: The database host to use
    :type db_host: str
    :param slack_token: The Slack token to use
    :type slack_token: str
    :param slack_channel: The Slack channel to use
    :type slack_channel: str
    :param slack_retries: The number of times to retry sending a Slack notification
    :type slack_retries: int
    :param slack_retry_delay: The number of seconds to wait between Slack retries
    :type slack_retry_delay: int
    """

    def __init__(
        self,
        s3_bucket: str,
        file_key: str,
        dry_run: str = None,
        s3_client: type = None,
        timestream_client: type = None,
        db_host: str = None,
        slack_token: str = None,
        slack_channel: str = None,
        slack_retries: int = 3,
        slack_retry_delay: int = 5,
    ) -> None:
        # Set dbhost to None
        if db_host is None:
            # Get DBHOST from environment variables
            db_host = os.getenv("SDC_AWS_DBHOST")

        if slack_token is None:
            # Get Slack Token from environment variables
            self.slack_token = os.getenv("SDC_AWS_SLACK_TOKEN")
        else:
            self.slack_token = slack_token

        if slack_channel is None:
            # Get Slack Channel from environment variables
            self.slack_channel = os.getenv("SDC_AWS_SLACK_CHANNEL")
        else:
            self.slack_channel = slack_channel

        if self.slack_token is not None and self.slack_channel is not None:
            # Initialize Slack Client
            self.slack_client = get_slack_client(self.slack_token)
        else:
            self.slack_client = None

        # Set Slack Retries
        self.slack_retries = slack_retries

        # Set Slack Retry Delay
        self.slack_retry_delay = slack_retry_delay

        if db_host is not None:
            # Intialize engine
            engine = create_engine(db_host)

            # Create tables if they don't exist
            create_tables(engine)

            # Set tracker to CDFTracker
            self.tracker = tracker.CDFTracker(engine, parser)
        else:
            # Set tracker to None
            self.tracker = None

        # Set File Key
        self.file_key = file_key

        # Set Instrument Bucket Name
        self.instrument_bucket_name = s3_bucket

        # Initialize timestream write client
        if timestream_client is not None:
            self.timestream_client = timestream_client
        else:
            self.timestream_client = create_timestream_client_session()

        # Initialize S3 client
        if s3_client is not None:
            self.s3_client = s3_client
        else:
            self.s3_client = create_s3_client_session()

        # Variable that determines if FileProcessor performs a Dry Run
        self.dry_run = dry_run
        if self.dry_run:
            log.warning("Performing Dry Run - Files will not be copied/removed")

        # Process File
        self._process_file()

    def _process_file(self) -> None:
        """
        Serve as the main entry point for the FileProcessor class.

        :return: None
        :rtype: None
        """

        file_path = None

        # Verify object exists in instrument bucket
        if (
            object_exists(
                self.s3_client,
                bucket=self.instrument_bucket_name,
                file_key=self.file_key,
            )
            or self.dry_run
        ):
            try:
                # Parse file key to get instrument name
                parsed_file_key = parse_file_key(self.file_key)

                # Download file from instrument bucket if not a dry run
                if not self.dry_run:
                    file_path = download_file_from_s3(
                        self.s3_client,
                        self.instrument_bucket_name,
                        self.file_key,
                        parsed_file_key,
                    )
                    if self.tracker:
                        self.tracker.track(file_path)

                # Parse the science file name
                science_file = parser(parsed_file_key)
                this_instr = science_file["instrument"]
                destination_bucket = INSTR_TO_BUCKET_NAME[this_instr]
                log.debug(
                    f"Destination Bucket Parsed Successfully: {destination_bucket}"
                )

                # Dynamically import instrument package
                instr_pkg = __import__(
                    f"{INSTR_TO_PKG[this_instr]}.calibration",
                    fromlist=["calibration"],
                )
                calibration = instr_pkg.calibration

                log.info(f"Using {INSTR_TO_PKG[this_instr]} module for calibration")

                # Process file
                try:
                    # Get name of new file
                    new_file_path = calibration.process_file(file_path)[0].name
                    # Get new file key
                    new_file_key = create_s3_file_key(parser, new_file_path)

                    # Upload file to destination bucket if not a dry run
                    if not self.dry_run:
                        # Upload file to destination bucket
                        path = upload_file_to_s3(
                            self.s3_client,
                            new_file_path,
                            destination_bucket,
                            new_file_key,
                        )
                        if self.tracker:
                            self.tracker.track(path)

                        if self.slack_client:
                            # Send Slack Notification
                            send_slack_notification(
                                slack_client=self.slack_client,
                                slack_channel=self.slack_channel,
                                slack_message=(
                                    f"File ({new_file_path}) has "
                                    "been successfully processed and "
                                    f"uploaded to {destination_bucket}.",
                                ),
                                alert_type="success",
                                slack_max_retries=self.slack_retries,
                                slack_retry_delay=self.slack_retry_delay,
                            )

                        # Log to timeseries database
                        log_to_timestream(
                            self.timestream_client,
                            action_type="PUT",
                            file_key=self.file_key,
                            new_file_key=new_file_key,
                            source_bucket=destination_bucket,
                            destination_bucket=destination_bucket,
                        )

                except ValueError as e:
                    log.error(e)

            except Exception as e:
                log.error(f"Error Processing File: {e}")
                if self.slack_client:
                    # Send Slack Notification
                    send_slack_notification(
                        slack_client=self.slack_client,
                        slack_channel=self.slack_channel,
                        slack_message=(
                            f"Error Processing File ({new_file_path})"
                            f"from {destination_bucket}.",
                        ),
                        alert_type="error",
                        slack_max_retries=self.slack_retries,
                        slack_retry_delay=self.slack_retry_delay,
                    )
                raise e
