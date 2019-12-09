import os
import time
import json
import shlex
import warnings
import copy
import string
from metaflow.metaflow_config import DATASTORE_SYSROOT_S3, SAGEMAKER_IAM_ROLE, SAGEMAKER_REGION

from requests.exceptions import HTTPError
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow import util, current
from time import strftime, gmtime

def get_sagemaker_environment():
    if SAGEMAKER_IAM_ROLE is None or SAGEMAKER_REGION is None:
        raise Exception("Ensure METAFLOW_SAGEMAKER_IAM_ROLE and METAFLOW_SAGEMAKER_REGION "
            "environment variables are set prior to execution.")
        
    s3_root =  "{}/{}/{}/{}/{}/sagemaker".format(DATASTORE_SYSROOT_S3,
                                     current.flow_name,
                                     current.run_id,
                                     current.step_name,
                                     current.task_id)

    return {"s3_root": s3_root,
        "sagemaker_iam": SAGEMAKER_IAM_ROLE,
        "sagemaker_region": SAGEMAKER_REGION}

class SageMakerParams(object):

    def __init__(self, s3_root, image, hyperparameters):

        self.RoleArn = SAGEMAKER_IAM_ROLE

        self.TrainingJobName = "{}-{}-{}".format(
            current.flow_name,
            current.run_id,
            current.step_name.replace("_", "-"))

        self.AlgorithmSpecification = {
            "TrainingImage": image,
            "TrainingInputMode": "File"
        }

        self.ResourceConfig = {
            "InstanceCount": 1,
            "InstanceType": "ml.m4.xlarge",
            "VolumeSizeInGB": 5
        }

        self.HyperParameters = hyperparameters

        self.StoppingCondition = {
            "MaxRuntimeInSeconds": 3600
        }

        self.OutputDataConfig = {
            'S3OutputPath': '{}/output'.format(s3_root)
        }

    def assemble_params(self, channels, s3_root, content_type, stopping_condition, resource_config):
        params = {}
        inputs = []
        for channel in channels:
            inputs.append({
                "ChannelName": channel,
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": "{}/{}".format(s3_root, channel),
                        "S3DataDistributionType": "FullyReplicated"
                    }
                },
                "ContentType": content_type,
                "CompressionType": "None"
                }
            )

        self.InputDataConfig = inputs

        if stopping_condition is not None:
            self.StoppingCondition = self.merge_sub_params(stopping_condition, self.StoppingCondition)

        if resource_config is not None:
            self.ResourceConfig = self.merge_sub_params(resource_config, self.ResourceConfig)

        for attr, value in self.__dict__.items():
            params[attr] = value
        return(params)

    def merge_sub_params(self, new_params, old_params):
            
        for key, value in new_params.items():
            if isinstance(value, dict):
                nested_dict = old_params.setdefault(key, {})
                self.merge_sub_params(value, old_params=nested_dict)
            else:
                old_params[key] = value
          
        return(old_params)
