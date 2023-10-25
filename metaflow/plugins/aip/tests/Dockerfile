# syntax = docker/dockerfile:experimental
# This image acts as both a CICD image used within the Gitlab runners run pytest which
# kicks off KFP runs using the Metaflow AIP plugin as well as the base image of Kubeflow
# pods running on the Kubeflow cluster.
FROM analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py39-cpu:5.0.2168.dev2168

# Add in AWS CLI because when we run the integration tests against the AI Analytics ZGCP clusters
# to authenticate with the Kubernetes API service we need to run "aws eks get-token" to generate the
# temporary token.
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"; \
    unzip -q awscliv2.zip; \
    ./aws/install; \
    rm -rf aws awscliv2.zip

ENV ARGO_VERSION=v3.4.1
RUN curl -sLO https://github.com/argoproj/argo-workflows/releases/download/${ARGO_VERSION}/argo-linux-amd64.gz; \
    gunzip argo-linux-amd64.gz; \
    chmod +x argo-linux-amd64; \
    sudo mv ./argo-linux-amd64 /usr/local/bin/argo

COPY . /home/zservice/metaflow

RUN --mount=type=cache,target=/home/zservice/.cache/pip \
    pip install --upgrade --force-reinstall \
    -i https://artifactory.zgtools.net/artifactory/api/pypi/analytics-python/simple \
    -e /home/zservice/metaflow[kfp,kfp-tests]

ARG IMAGE_TAG
ENV IMAGE_TAG=$IMAGE_TAG
