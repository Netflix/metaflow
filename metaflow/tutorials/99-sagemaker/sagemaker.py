import os
import io

from six.moves.urllib.parse import urlparse

import boto3

from metaflow import FlowSpec, step, conda, conda_base, Sagemaker, current

class XORFlow(FlowSpec):
    """
    A flow for Metaflow to demonstrate Sagemaker training.

    Run this flow to learn how to enable Metaflow to speak to Amazon Sagemaker.

    """
    @step
    def start(self):
        os.system("pip install pandas")

        import pandas as pd
        
        data = [[0, 0, 0],
                [0, 1, 1],
                [1, 1, 0],
                [1, 0, 1]]

        xor = pd.DataFrame(data, columns = ['Result', 'Input1', 'Input2'])

        for i in range(1, 10):
            xor = xor.append(xor.sample(frac=1))

        self.data = xor
        self.endpoints = []

        self.next(self.split)

    @step
    def split(self):
        """
        Fork two parallel branches.
        """

        self.next(self.xgb_branch, self.ll_branch)

    @step
    def xgb_branch(self):
        """
        Train and deploy the XOR model with Sagemaker XGBoost
        """
        self.branch = 'xgb'
        os.system("pip install pandas")

        import pandas as pd
        import numpy as np

        ## Sagemaker Image and Hyperparameters below are required fields.
        ## Common parameters and image info can be found at https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-algo-docker-registry-paths.html
        sagemaker_image = "433757028032.dkr.ecr.us-west-2.amazonaws.com/xgboost:latest"
        hyperparameters =  {
            "max_depth":"5",
            "eta":"0.2",
            "gamma":"4",
            "min_child_weight":"6",
            "silent":"0",
            "objective": "binary:logistic",
            "num_round": "100",
            "subsample" : "0.8"
        }

        ## Resource config is an optional parameter
        resource_config = {
            "InstanceCount": 5
        }

        rand_split = np.random.rand(len(self.data))
        train_list = rand_split < 0.8
        val_list = rand_split >= 0.8

        train_data = self.data[train_list]
        val_data = self.data[val_list]

        data={}
        data['train'] = train_data.to_csv(index=False, header=False)
        data['validation'] = val_data.to_csv(index=False, header=False)

        model_uri = Sagemaker.fit(data, sagemaker_image, hyperparameters, resource_config=resource_config)
        self.endpoint = Sagemaker.deploy(model_uri, sagemaker_image)
        
        self.next(self.join)

    @step
    def ll_branch(self):
        """
        Train and deploy the XOR model with Sagemaker Linear Learner
        """
        self.branch = 'll'
        os.system("pip install pandas")

        import pandas as pd
        import numpy as np

        ## Sagemaker Image and Hyperparameters below are required fields.
        ## Common parameters and image info can be found at https://docs.aws.amazon.com/sagemaker/latest/dg/sagemaker-algo-docker-registry-paths.html
        sagemaker_image = "174872318107.dkr.ecr.us-west-2.amazonaws.com/linear-learner:latest"
        hyperparameters =  {
            "feature_dim": "2",
            "mini_batch_size": "100",
            "predictor_type": "binary_classifier",
            "epochs": "10",
            "num_models": "32",
            "loss": "logistic"
        }

        ## Resource config is an optional parameter
        resource_config = {
            "InstanceCount": 5,
            "InstanceType": "ml.c4.2xlarge",
            "VolumeSizeInGB": 10
        }

        rand_split = np.random.rand(len(self.data))
        train_list = rand_split < 0.7
        val_list = (rand_split >= 0.7) & (rand_split < 0.9)
        test_list = rand_split >= 0.9

        train_data = self.data[train_list]
        val_data = self.data[val_list]
        test_data = self.data[test_list]

        data={}
        data['train'] = train_data.to_csv(index=False, header=False)
        data['validation'] = val_data.to_csv(index=False, header=False)
        data['test'] = test_data.to_csv(index=False, header=False)

        model_uri = Sagemaker.fit(data, sagemaker_image, hyperparameters, resource_config=resource_config)
        self.endpoint = Sagemaker.deploy(model_uri, sagemaker_image)

        self.next(self.join)

    @step
    def join(self, inputs):
        """
        Join branches and merge their results.
        """
        os.system("pip install pandas")

        import pandas as pd

        self.branches = []
        for i in inputs:
            self.branches.append(i.branch)

        self.endpoints = []
        for i in inputs:
            self.endpoints.append(i.endpoint)
        
        self.prediction_data = inputs[0].data.sample(n=100, random_state=5)

        self.next(self.end)

    @step
    def end(self):
        """
        Let's run some Batch predictions and see how our models do.
        """
        os.system("pip install pandas sklearn")

        from sklearn.metrics import roc_auc_score
        import pandas as pd

        prediction_labels = self.prediction_data['Result'].values.tolist()
        prediction_labels = [str(i) for i in prediction_labels]

        prediction_features = self.prediction_data.drop('Result', axis=1).values.tolist()
        prediction_features = [','.join([(str(j)) for j in i]) for i in prediction_features]

        predictions = {}

        for endpoint in self.endpoints:
            predictions[endpoint] = []
            for feature in prediction_features:
                predictions[endpoint].append(Sagemaker.predict(feature, endpoint))

        for endpoint in predictions:
            print('{} score: {}'.format(endpoint, roc_auc_score(prediction_labels, predictions[endpoint])))

        self.myval = 42

if __name__ == '__main__':
    XORFlow()
