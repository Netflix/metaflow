import os
import time
import json
import shlex
import warnings
import boto3
import string

from requests.exceptions import HTTPError
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow import util, current, S3

from .sagemaker_params import SageMakerParams, get_sagemaker_environment

class SageMakerClient(object):

    @classmethod
    def fit(cls, data, image, hyperparameters, content_type="text/csv", stopping_condition=None, resource_config=None):

        # Retrieve Sagemaker environment info and S3 paths
        aws_env = get_sagemaker_environment()

        channels = []
        for channel in data:
            channels.append(channel)
            with S3(s3root="{}/{}".format(aws_env['s3_root'], channel)) as s3:
                message = data[channel]
                s3.put('{}.csv'.format(channel), message)

        # Sagemaker code.
        params = SageMakerParams(aws_env['s3_root'], image, hyperparameters)
        params_complete = params.assemble_params(channels, aws_env['s3_root'], content_type, stopping_condition, resource_config)

        print("Sagemaker Training Starting...  Please wait.")

        sm = boto3.Session().client('sagemaker', region_name=aws_env['sagemaker_region'])
        sm.create_training_job(**params_complete)

        training_job_name = params.TrainingJobName

        sm.get_waiter('training_job_completed_or_stopped').wait(TrainingJobName=training_job_name)
        status = sm.describe_training_job(TrainingJobName=training_job_name)['TrainingJobStatus']
        print("Training job ended with status: " + status)
        if status == 'Failed':
            message = sm.describe_training_job(TrainingJobName=training_job_name)['FailureReason']
            print('Training failed with the following error: {}'.format(message))
            raise Exception('Training job failed')

        return("{}/{}/output/model.tar.gz".format(params.OutputDataConfig['S3OutputPath'], training_job_name))

    @classmethod
    def deploy(cls, model_uri, image, instanceType="ml.m4.xlarge", instanceCount = 1, instanceWeight = 1, variantName="AllTraffic" ):

         # Retrieve Sagemaker environment info and S3 paths
        aws_env = get_sagemaker_environment()

        model_root = "{}-{}-{}".format(
            current.flow_name,
            current.run_id,
            current.step_name.replace("_", "-"))

        sm = boto3.Session().client('sagemaker', region_name=aws_env['sagemaker_region'])
        container = {
            'Image': image,
            'ModelDataUrl': model_uri,
            'Environment': {'this': 'is'}
        }

        model_response = sm.create_model(
            ModelName="{}-Model".format(model_root),
            ExecutionRoleArn=aws_env['sagemaker_iam'],
            PrimaryContainer=container)

        endpoint_config_response = sm.create_endpoint_config(
            EndpointConfigName = "{}-Endpt-Config".format(model_root),
            ProductionVariants=[{
                'InstanceType': instanceType,
                'InitialInstanceCount': instanceCount,
                'InitialVariantWeight': instanceWeight,
                'ModelName':"{}-Model".format(model_root),
                'VariantName': variantName}])

        endpoint_name = "{}-Endpt".format(model_root)
        endpoint_response = sm.create_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName="{}-Endpt-Config".format(model_root))

        resp = sm.describe_endpoint(EndpointName=endpoint_name)
        status = resp['EndpointStatus']
        print("Status: " + status)

        while status=='Creating':
            time.sleep(60)
            resp = sm.describe_endpoint(EndpointName=endpoint_name)
            status = resp['EndpointStatus']
            print("Status: " + status)

        print("Arn: " + resp['EndpointArn'])
        print("Status: " + status)

        return(endpoint_name)

    @classmethod
    def predict(cls, data, endpoint_name, content_type="text/csv"):

         # Retrieve Sagemaker environment info and S3 paths
        aws_env = get_sagemaker_environment()
        
        runtime= boto3.Session().client('runtime.sagemaker', region_name=aws_env['sagemaker_region'])

        response = runtime.invoke_endpoint(EndpointName=endpoint_name, 
                                    ContentType=content_type, 
                                    Body=data)
        # hack Linear learner inference response is application/json
        if response['ContentType']=='application/json':
            result = json.loads(response['Body'].read().decode("utf-8"))
            preds =  [r['score'] for r in result['predictions']]
        else:
            result = response['Body'].read()
            result = result.decode("utf-8")
            result = result.split(',')
            preds = [float((num)) for num in result]
        
        return(preds)