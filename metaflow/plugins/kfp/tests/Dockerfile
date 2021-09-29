# This image acts as both a CICD image used within the Gitlab runners run pytest which
# kicks off KFP runs using the Metaflow KFP plugin as well as the base image of Kubeflow
# pods running on the Kubeflow cluster.
FROM analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py39-cpu-cicd:4.0.1380
COPY . /home/zservice/metaflow
RUN pip install --upgrade --force-reinstall -e /home/zservice/metaflow
RUN pip install \
    pytest==6.1.2 \
    pytest-xdist==2.1.0 \
    pytest-cov==2.10.1 \
    subprocess-tee==0.3.2 \
    pyyaml==5.4.1 \
    kubernetes==18.20.0
ARG IMAGE_TAG
ENV IMAGE_TAG=$IMAGE_TAG
