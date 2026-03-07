FROM python:3.9

# Pre-install boto3 and requests so Metaflow steps skip the per-container
# pip install (METAFLOW_SKIP_INSTALL_DEPENDENCIES remains unset, but these
# are already present). Saves ~2 minutes per kubernetes/batch pod in devstack.
RUN pip install --no-cache-dir boto3 requests
