from aws_cdk import Stack, aws_lambda, aws_ecr
from constructs import Construct
import logging


class SDCAWSProcessingLambdaStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ECR Repo Name
        ecr_repository = "sdc_aws_processing_lambda"

        

        # Create Container Image ECR Function
        sdc_aws_processing_function = aws_lambda.DockerImageFunction(
            scope=self,
            id=f"{repo_name}_function",
            function_name=f"{repo_name}_function",
            description=(
                "SWSOC Processing Lambda function deployed using AWS CDK Python"
            ),
            code=aws_lambda.DockerImageCode.from_ecr(ecr_repository),
        )

        logging.info("Function created successfully: %s", sdc_aws_processing_function)
