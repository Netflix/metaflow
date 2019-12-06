# Episode 99-sagemaker: Let's do a simple workflow with Amazon Sagemaker.
--
**This flow creates an XOR dataset and trains and deploys it against two different Sagemaker algorithms**
**Please note that this flow requires a user with access to AWS Sagemaker permissions**

--

#### Showcasing:
- Sagemaker Capabilities
- Static branching
- Join step

#### To play this episode:
1. Determine your AWS region and find your appropriate XGBoost and Linear Learner Images and insert them into the "sagemaker_image" variables in the flow: https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-algo-docker-registry-paths.html

2. Alternatively, run this in us-west-2 with the existing parameters.

3. ```cd metaflow-tutorials/99-sagemaker```

4. ```python sagemaker.py show```

5. ```python sagemaker.py run```
