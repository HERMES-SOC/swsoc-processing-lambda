import logging

# import os

# Configure logging
log = logging.getLogger()

# Format for log file entries log_file_format = %(asctime)s, %(origin)s, %(levelname)s, %(message)s
log_file_format = "%(asctime)s, %(origin)s, %(levelname)s, %(message)s"

# # If LAMDA_ENVIRONMENT is set to "PRODUCTION" then log info and above to file
# if (
#     "LAMBDA_ENVIRONMENT" in os.environ
#     and os.environ["LAMBDA_ENVIRONMENT"] == "PRODUCTION"
# ):
#     log.setLevel(logging.INFO)
#     log_file = f"/tmp/sdc_aws_processing_lambda.log"
#     fh = logging.FileHandler(log_file)
#     fh.setLevel(logging.INFO)
#     formatter = logging.Formatter(log_file_format)
# else:
#     log.setLevel(logging.DEBUG)
#     fh = logging.StreamHandler()

# To remove boto3 noisy debug logging
logging.getLogger("botocore").setLevel(logging.CRITICAL)
logging.getLogger("boto3").setLevel(logging.CRITICAL)
