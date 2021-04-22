FROM analytics-docker.artifactory.zgtools.net/artificial-intelligence/ai-platform/aip-py36-cpu-cicd:3.2.25c62576.master
COPY . /home/zservice/metaflow
RUN pip install --upgrade --no-deps --force-reinstall -e /home/zservice/metaflow
RUN pip install pytest==6.1.2 pytest-xdist==2.1.0 pytest-cov==2.10.1
