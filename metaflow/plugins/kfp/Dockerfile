# Used to create the ghcr.io/zillow/metaflow/metaflow-zillow:2.1 image.
# The image is used in two places:
#   1. In metaflow/plugins/kfp/kfp_constants.py, where it is used as a base image
#      when the user does not provide their own base image with python flow.py kfp run --base-image...
#   2. In metaflow/plugins/kfp/kfp.py, where it is used as the base image of the 
#      s3_sensor_op.
# Steps to build and push new versions of this image:
# 1. Go to Github and create a personal access token that allows you to write packages
# link: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
# 2. cd in this directory, and build the image: `docker build .`
# 3. run `docker images` -> note the image ID of the image you just created
# 4. run `docker tag [image_id] ghcr.io/zillow/metaflow/metaflow-zillow:[version]`
# 5. push to Github: `docker push ghcr.io/zillow/metaflow/metaflow-zillow`

FROM python:3.9.0

WORKDIR /opt/zillow

RUN apt-get update && apt-get install -yq --no-install-recommends \
        apt-transport-https \
        apt-utils \
        bash \
        build-essential \
        ca-certificates \
        curl \
        dialog \
        git \
        isomd5sum \
        libffi-dev \
        libmpdec-dev \
        libssl-dev \
        sudo \
        vim \
        wget \
        zip \
        zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip & install poetry
RUN pip install -U pip==20.0.2 poetry==1.0.10
ENV POETRY_VIRTUALENVS_CREATE=False

RUN pip install awscli click requests boto3 pytest pytest-xdist

# Create Z_USER with UID=1000 and in the 'users' group
ARG Z_USER="zservice"
ARG Z_UID="1000"
ARG Z_GID="100"

# Configure environment
ENV SHELL=/bin/bash \
    Z_USER=$Z_USER \
    Z_UID=$Z_UID \
    Z_GID=$Z_GID \
    HOME=/home/$Z_USER

# Enable users to install packages as root
RUN echo "%users ALL=(ALL) NOPASSWD:/usr/local/bin/pip" >> /etc/sudoers && \
    echo "%users ALL=(ALL) NOPASSWD:/usr/local/bin/pip3" >> /etc/sudoers && \
    echo "%users ALL=(ALL) NOPASSWD:/usr/local/bin/python" >> /etc/sudoers && \
    echo "%users ALL=(ALL) NOPASSWD:/usr/bin/apt-get" >> /etc/sudoers && \
    echo "%users ALL=(ALL) NOPASSWD:/usr/bin/apt" >> /etc/sudoers
