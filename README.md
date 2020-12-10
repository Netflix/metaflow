## Metaflow-Argo plugin
Runs [Metaflow](https://metaflow.org/) pipelines on [Argo Workflows](https://argoproj.github.io/projects/argo).

### Getting Started

* Install Metaflow from the current repo which includes the argo plugin:
  ```
  pip install git+ssh://git@github.wdf.sap.corp/AI/metaflow-argo.git@argo
  ```

* To run flows on the Argo Workflows, you need to [Configure S3 and Argo Server](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Configure-Metaflow-Argo).
 
* Access tutorials by typing:
  ```
  metaflow tutorials pull
  ```
  See [Tutorials](https://docs.metaflow.org/getting-started/tutorials)

* MNIST Argo Workflows tutorial:
  ```
  metaflow tutorials info mnist
  ```

### Links

You can find more documentation here:

* [Metaflow](https://github.com/Netflix/metaflow)
* [Conda](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Conda)
* [Local installation of Argo](https://github.wdf.sap.corp/AI/metaflow-argo/wiki/Argo)
