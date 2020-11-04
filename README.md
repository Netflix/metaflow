## Metaflow-Argo plugin

The aim of this plugin is to generate the yaml which is runnable in argo based on the metaflow pipeline.

In order to see how metaflow works and how you can setup argo in your local machine to run the generated-yaml 
on argo, you can find a documentation on the following link:

1. [Metaflow](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Metaflow)
2. [Argo](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Argo)
3. [argo-plugin](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/argo-plugin)

### Getting Started

`git clone git@github.wdf.sap.corp:AI/metaflow-argo.git`

Install Metaflow from this repo which includes the argo plugin:\
`python3 setup.py install`

Then you can run your metaflow pipline as follows:\
`python3 <YOUR-SCRIPT>.py argo create -only-yaml > <FILE>.yaml`

here is an example that includes argo decorator.\
`python3 metaflow/tutorials/mnist/mnist-training.py argo create -only-yaml > mnist.yaml`

At the end submit the generated yaml to argo dashboard.

## Points of Contact
* Developer: [Roman Kindruk](roman.kindruk@sap.com)
* Developer: [Karim Mohraz](karim.mohraz@sap.com)
* Developer: [Martin Kraemer](martin.kraemer@sap.com)
* Developer: [Zahra Zamansani](zahra.zamansani@sap.com)
