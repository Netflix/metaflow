# Running Metaflow on On‑Premises Kubernetes

This guide shows how to deploy and use **Metaflow** on your own Kubernetes cluster, whether it’s on‑premises or on a cloud provider other than AWS.

---

## Overview

Metaflow supports any Kubernetes cluster, not just AWS EKS.  
This guide covers:

- Prerequisites and requirements  
- Configuration steps  
- Storage setup (using **MinIO** as S3‑compatible storage)  
- Example flows  
- Troubleshooting  

---

## Prerequisites

1. **Kubernetes cluster**  
   - Version ≥ 1.19  
   - `kubectl` configured with cluster access  
   - Recommended: ≥ 3 nodes  

2. **Python environment**  
   ```bash
   python3 -m pip install metaflow kubernetes
   ```
(Requires Python 3.7+)

3. **Storage solution**  
   - S3‑compatible object storage (MinIO in this guide)  
   - Persistent‑volume provisioner for the metadata database  


---

### Step 1 – Set up S3‑Compatible Storage (MinIO)

Create the namespace, Deployment, Service, and PVC for MinIO:

```yaml
# minio-deployment.yaml

apiVersion: v1
kind: Namespace
metadata:
  name: metaflow
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: metaflow
spec:
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
        - name: minio
          image: minio/minio:latest
          imagePullPolicy: IfNotPresent
          args: ["server", "/data"]
          env:
            - name: MINIO_ROOT_USER
              value: minioadmin
            - name: MINIO_ROOT_PASSWORD
              value: minioadmin123
          ports:
            - containerPort: 9000
          volumeMounts:
            - name: storage
              mountPath: /data
      volumes:
        - name: storage
          persistentVolumeClaim:
            claimName: minio-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: metaflow
spec:
  selector:
    app: minio
  ports:
    - port: 9000
      targetPort: 9000
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-pvc
  namespace: metaflow
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 100Gi
```
Apply the manifest:

```bash
kubectl apply -f minio-deployment.yaml
```
---

### Step 2 – Deploy the Metadata Service

```yaml
# metadata-service.yaml

apiVersion: apps/v1
kind: Deployment
metadata:
  name: metadata-service
  namespace: metaflow
spec:
  replicas: 1
  selector:
    matchLabels:
      app: metadata-service
  template:
    metadata:
      labels:
        app: metadata-service
    spec:
      containers:
        - name: metadata-db
          image: postgres:13
          env:
            - name: POSTGRES_DB
              value: metaflow
            - name: POSTGRES_USER
              value: metaflow
            - name: POSTGRES_PASSWORD
              value: metaflow123
          ports:
            - containerPort: 5432
          volumeMounts:
            - name: postgres-storage
              mountPath: /var/lib/postgresql/data
               subPath: pgdata
        - name: metadata-service
          image: netflixoss/metaflow_metadata_service:latest
          env:
            - name: MF_METADATA_DB_HOST
              value: localhost
            - name: MF_METADATA_DB_PORT
              value: "5432"
            - name: MF_METADATA_DB_USER
              value: metaflow
            - name: MF_METADATA_DB_PSWD
              value: metaflow123
            - name: MF_METADATA_DB_NAME
              value: metaflow
          ports:
            - containerPort: 8080
      volumes:
        - name: postgres-storage
          persistentVolumeClaim:
            claimName: metadata-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: metadata-service
  namespace: metaflow
spec:
  selector:
    app: metadata-service
  ports:
    - port: 8080
      targetPort: 8080
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: metadata-pvc
  namespace: metaflow
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 20Gi
```

Apply the manifest:

```bash
kubectl apply -f metadata-service.yaml
```

---

### Step 3 – Configure Metaflow

Run the interactive wizard:

```bash
metaflow configure kubernetes
```

**Wizard prompts**

| Prompt               | Example answer               |
| -------------------- | ---------------------------- |
| Kubernetes namespace | `metaflow`                   |
| Service account      | `default` (or dedicated SA)  |
| Container registry   | `<your‑registry>` (optional) |
| Base image           | `<python‑base‑image>` (opt.) |


Set environment variables for MinIO and the metadata service:

```bash

export METAFLOW_DEFAULT_DATASTORE=s3
export METAFLOW_S3_ENDPOINT_URL=http://minio.metaflow.svc.cluster.local:9000
export METAFLOW_S3_ACCESS_KEY_ID=minioadmin
export METAFLOW_S3_SECRET_ACCESS_KEY=minioadmin123
export METAFLOW_DEFAULT_METADATA=service
export METAFLOW_SERVICE_URL=http://metadata-service.metaflow.svc.cluster.local:8080
export METAFLOW_DATASTORE_SYSROOT_S3=s3://metaflow-data
```

---
### Step 4 – Create a MinIO Bucket

```bash

# Port‑forward MinIO
kubectl port-forward -n metaflow svc/minio 9000:9000 &
# Install mc (macOS example)
brew install minio/stable/mc
# Configure and create bucket
mc alias set myminio http://localhost:9000 minioadmin minioadmin123
mc mb myminio/metaflow-data
```

---
### Step 5 – Run Your First Flow

```python

from metaflow import FlowSpec, step, kubernetes
import socket

class HelloK8sFlow(FlowSpec):

    @step
    def start(self):
        print("Starting on local machine")
        self.message = "Hello from on‑prem K8s!"
        self.next(self.process)

    @kubernetes(cpu=1, memory=500)
    @step
    def process(self):
        print(f"Processing in Kubernetes: {self.message}")
        self.hostname = socket.gethostname()
        self.next(self.end)

    @step
    def end(self):
        print(f"Completed! Pod {self.hostname}: {self.message}")

if __name__ == "__main__":
    HelloK8sFlow()
```

Run it:

```bash
python hello_k8s.py run
```
---
## (Optional) GPU example

```python
@kubernetes(gpu=1, memory=8000)
@step
def train_model(self):
    import torch
    print("CUDA available:", torch.cuda.is_available())
    if torch.cuda.is_available():
        print("GPU:", torch.cuda.get_device_name(0))
```

---
## Using Custom Images

```python

@kubernetes(image="myregistry/my-ml-image:latest", gpu=1)
@step
def custom_step(self):
    pass
```

---
## Production Considerations

### Security
- RBAC for the Metaflow service account
- Strong MinIO credentials
- TLS everywhere


### High availability
- Multiple replicas of the metadata service
- Managed or HA PostgreSQL
- Distributed MinIO

### Monitoring
- Prometheus + Grafana
- Kubernetes events & logs


### Storage classes

```yaml
# Example local storage class
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: local-storage
provisioner: kubernetes.io/no-provisioner
volumeBindingMode: WaitForFirstConsumer
```

---
## Troubleshooting

| Issue                    | Suggested command or action                                   |
| ------------------------ | ------------------------------------------------------------- |
| Pod fails to start       | `kubectl logs -n metaflow <pod>`                              |
| Storage connection errors| `curl http://minio:9000` from a Metaflow pod                  |
| Metadata service errors  | `kubectl logs -n metaflow deployment/metadata-service`        |


---
### RKE2‑Specific Notes (Issue #2366)

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: nvidia-device-plugin-daemonset
  namespace: kube-system
spec:
  selector:
    matchLabels:
      name: nvidia-device-plugin-ds
  template:
    metadata:
      labels:
        name: nvidia-device-plugin-ds
    spec:
      tolerations:
        - key: nvidia.com/gpu
          operator: Exists
          effect: NoSchedule
      containers:
        - name: nvidia-device-plugin-ctr
          image: nvcr.io/nvidia/k8s-device-plugin:v0.14.0
          securityContext:
            allowPrivilegeEscalation: false
            capabilities:
              drop: ["ALL"]
          volumeMounts:
            - name: device-plugin
              mountPath: /var/lib/kubelet/device-plugins
      volumes:
        - name: device-plugin
          hostPath:
            path: /var/lib/kubelet/device-plugins
```

### Ceph storage class
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ceph-rbd
provisioner: rbd.csi.ceph.com
parameters:
  clusterID: <your-ceph-cluster-id>
  pool: <your-ceph-pool>
  csi.storage.k8s.io/provisioner-secret-name: ceph-secret
  csi.storage.k8s.io/provisioner-secret-namespace: default
```

### Next Steps
- Integrate Argo Workflows for production orchestration
- Set up HPA/VPA for autoscaling
- Add CI/CD integration
- Enable monitoring and alerting


---
### Need Help?

- **Slack** – Join the [Metaflow Community Slack](https://metaflow.org/slack)
- **Discussions** – Browse [GitHub Discussions](https://github.com/Netflix/metaflow/discussions)
- **Issues** – [Create a new issue](https://github.com/Netflix/metaflow/issues)
