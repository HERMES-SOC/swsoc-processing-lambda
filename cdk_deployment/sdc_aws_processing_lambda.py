from aws_cdk import (
    Stack,
    aws_lambda,
    aws_ecr,
    aws_ecr_assets,
    aws_iam as iam,
)
import json
from constructs import Construct
import logging
import os
class SDCAWSProcessingLambdaStack(Stack):

   def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        repo_name = 'sdc_aws_processing_function'

        ecr_repository = aws_ecr.Repository(
                            self, 
                            id=f"{repo_name}_repo", 
                            repository_name=repo_name,
                            )
        
        login_ecr_command = f"aws ecr get-login-password --region {os.getenv('CDK_DEFAULT_REGION')} | docker login --username AWS --password-stdin {ecr_repository.repository_uri}"
        docker_build_command = f'cd function && docker build -t {repo_name}:latest .'
        docker_tag_command = f"docker tag sdc_aws_processing_lambda:latest {ecr_repository.repository_uri}/sdc_aws_processing_lambda:latest"
        # push_ecr_command = f""
        # docker_tag_command = 
        os.system(docker_build_command)
        logging.warning(login_ecr_command)
        ### Create Cognito Remediator Lambda function
        sdc_aws_processing_function = aws_lambda.DockerImageFunction(
            scope=self,
            id=f"{repo_name}_function",
            function_name=f"{repo_name}_function",
            description="SWSOC Processing Lambda function deployed using AWS CDK Python",
            code=aws_lambda.DockerImageCode.from_ecr(ecr_repository),
        )



