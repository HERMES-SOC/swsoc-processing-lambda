from aws_cdk import (
    Stack,
    aws_lambda,
    aws_ecr,
    aws_ecr_assets,
    aws_iam as iam,
)
import json
from constructs import Construct
import base64, docker, boto3
import logging
class SDCAWSProcessingLambdaStack(Stack):

   def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        repo_name = ''
        with open('cdk.json') as json_file:
            config = json.load(json_file)
            repo_name = config['context']['assets-ecr-repository-name']

        ecr_repository = aws_ecr.Repository(
                            self, 
                            id=f"{repo_name}_repo", 
                            repository_name=repo_name,
                            )

        docker_client = docker.from_env(version='1.24')
        ecr_client = boto3.client('ecr', region_name='us-east-2')

        token = ecr_client.get_authorization_token()
        username, password = base64.b64decode(token['authorizationData'][0]['authorizationToken']).decode().split(':')
        registry = token['authorizationData'][0]['proxyEndpoint']

        docker_client.login(username, password, registry=registry)
        logging.warning(token)
        print(registry)
        ### Create Cognito Remediator Lambda function
        sdc_aws_processing_function = aws_lambda.DockerImageFunction(
            scope=self,
            id=f"{repo_name}_function",
            function_name=f"{repo_name}_function",
            description="SWSOC Processing Lambda function deployed using AWS CDK Python",
            code=aws_lambda.DockerImageCode.from_ecr(ecr_repository),
        )



