# Repo where this image's Dockerfile is maintained: https://github.com/HERMES-SOC/docker-lambda-base
ARG IMAGE_TAG=latest
ARG IMAGE_NAME=dev-swsoc-docker-lambda-base
FROM public.ecr.aws/w5r9l1c8/${IMAGE_NAME}:${IMAGE_TAG}

# Working Directory Arguments
ARG ROOT="/"
ARG FUNCTION_DIR="/lambda_function/"

COPY requirements.txt ${ROOT}

# Update pip and install setuptools & Install requirements
RUN pip install --no-cache-dir --upgrade pip setuptools && \
    pip install --no-cache-dir --upgrade -r requirements.txt

# Install libpq
RUN apt-get update && \
    apt-get install -y libpq-dev

# Change working directory to /function
WORKDIR ${ROOT}

# Set Up Lambda Runtime Environment
RUN curl -Lo /usr/local/bin/aws-lambda-rie \
    https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie && \
    chmod +x /usr/local/bin/aws-lambda-rie


# Copy entry script into function director (Script is used distinguish dev/production mode)
COPY entry_script.sh ${ROOT}

# Copy files from the source folder
COPY lambda_function/ ${FUNCTION_DIR}

# Copy Config
COPY config.yaml ${ROOT}

# Runs entry script to decide wether to run function in local environment or in production environment
ENTRYPOINT [ "sh", "entry_script.sh" ]

# Runs lambda handler function
CMD [ "lambda_function.handler.handler_function" ]
