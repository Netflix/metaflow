# Episode 08-sagemaker: Let's do a simple workflow with Amazon Sagemaker.

**This flow creates an XOR dataset and trains and deploys it against two different Sagemaker algorithms
Please note that this flow requires a user with access to AWS Sagemaker permissions**

#### Showcasing:
- Sagemaker Capabilities
- Conda Dependency Management
- Static branching
- Join step

#### Before playing this episode:
1. Configure your sandbox: https://docs.metaflow.org/metaflow-on-aws/metaflow-sandbox
2. Determine your AWS region and required XGBoost and Linear Learner Images by visiting https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-algo-docker-registry-paths.html
3. Insert the image address into all instances of the ```sagemaker_image``` variable in the sagemaker.py flow.
4. Alternatively, run this in us-west-2 with the existing parameters.

#### Sagemaker-specific environment variables required for this flow:
1. ```export METAFLOW_SAGEMAKER_REGION="<YOUR_AWS_REGION>"```
2. ```export METAFLOW_SAGEMAKER_IAM_ROLE="<SAGEMAKER_IAM_ARN_WITH_S3_PERMISSIONS_FROM_CFN>"```

#### To play this episode:

1. ```cd metaflow-tutorials/08-sagemaker```

2. ```python sagemaker.py --environment=conda show```

3. ```python sagemaker.py --environment=conda run --with batch```
