
from .s3 import S3
from .local import Local

DATACLIENTS = {'local': Local,
               's3': S3}