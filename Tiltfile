# Tilt configuration for running Metaflow on a local Kubernetes stack
#
# TODO: Document all components
#
# Usage:
#   Start the development environment:
#     $ tilt up
#   Stop and clean up:
#     $ tilt down

# TODO:
# 1. helm repo update can be slow
# 2. pass selected components to tilt up
# 

version_settings(constraint='>=0.22.2')
allow_k8s_contexts('minikube')

components = {
    "metadata-service": ["postgresql"],
    "ui": ["metadata-service", "minio"],
    "minio": [],
    "postgresql": [],
    "argo-workflows": [],
    "argo-events": ["argo-workflows"],
    "jobset": ["kueue"], # for gang-scheduled workloads
    "kueue": [],
    
}

# TODO: Make this work with a list of services

requested_components = list(components.keys())

# TODO: Get this from makefile
local_config_dir = ".devtools"
def ensure_local_config_dir():
    local(shell_cmd="mkdir -p {}".format(local_config_dir))
metaflow_config = {}
aws_config = []

load('ext://helm_resource', 'helm_resource', 'helm_repo')
load('ext://helm_remote', 'helm_remote')


def resolve(component, resolved=None):
    if resolved == None:
        resolved = []
    if component in resolved:
        return resolved
    if component in components:
        for dep in components[component]:
            resolve(dep, resolved)
    resolved.append(component)
    return resolved

valid_components = []
for component in components.keys():
    if component not in valid_components:
        valid_components.append(component)
for deps in components.values():
    for dep in deps:
        if dep not in valid_components:
            valid_components.append(dep)

enabled_components = []
for component in requested_components:
    if component not in valid_components:
        fail("Unknown component: " + component)
    for result in resolve(component):
        if result not in enabled_components:
            enabled_components.append(result)

# Print a friendly summary when running `tilt up`.            
if config.tilt_subcommand == 'up':
    print("\n📦 Components to install:")
    for component in enabled_components:
        print("• " + component)
        if component in components and components[component]:
            print("  ↳ requires: " + ", ".join(components[component]))

#################################################
# MINIO
#################################################
if "minio" in enabled_components:
    helm_remote(
        'minio',
        repo_name='minio',
        repo_url='https://charts.bitnami.com/bitnami',
        set=[
            'auth.rootUser=rootuser',
            'auth.rootPassword=rootpass123',
            'defaultBuckets=metaflow-test',
            'persistence.enabled=false',
            'ingress.enabled=false',
            'consoleIngress.enabled=false',
            'resources.requests.memory=128Mi',
            'resources.requests.cpu=50m',
            'resources.limits.memory=256Mi',
            'resources.limits.cpu=100m',
            'startupProbe.initialDelaySeconds=1',
            'livenessProbe.initialDelaySeconds=1',
            'readinessProbe.initialDelaySeconds=1'
        ]
    )

    k8s_resource(
        'minio',
        port_forwards=[
            '9000:9000',
            '9001:9001'
        ],
        links=[
            link('http://localhost:9000', 'MinIO API'),
            link('http://localhost:9001/login', 'MinIO Console (rootuser/rootpass123)')
        ],
        labels=['minio'],
        resource_deps=['minio-secret']
    )

    k8s_yaml(encode_yaml({
        'apiVersion': 'v1',
        'kind': 'Secret',
        'metadata': {'name': 'minio-secret'},
        'type': 'Opaque',
        'stringData': {
            'AWS_ACCESS_KEY_ID': 'rootuser',
            'AWS_SECRET_ACCESS_KEY': 'rootpass123',
            'AWS_ENDPOINT_URL': 'http://minio:9000'
        }
    }))

    # Metaflow config overrides for MinIO usage
    metaflow_config["METAFLOW_DEFAULT_DATASTORE"] = "s3"
    metaflow_config["METAFLOW_DATASTORE_SYSROOT_S3"] = "s3://metaflow-test/metaflow"

    aws_config = """[metaflow-local-minio]
aws_access_key_id = rootuser
aws_secret_access_key = rootpass123
region = us-east-1
s3 =
    endpoint_url = http://localhost:9000

"""

#################################################
# POSTGRESQL
#################################################
if "postgresql" in enabled_components:
    helm_remote(
        'postgresql',
        version='12.5.6',
        repo_name='postgresql',
        repo_url='https://charts.bitnami.com/bitnami',
        set=[
            'auth.username=metaflow',
            'auth.password=metaflow',
            'auth.database=metaflow',
            'primary.persistence.enabled=false',
            'primary.resources.requests.memory=128Mi',
            'primary.resources.requests.cpu=50m',
            'primary.resources.limits.memory=256Mi',
            'primary.resources.limits.cpu=100m',
            'primary.terminationGracePeriodSeconds=1',
            'primary.podSecurityContext.enabled=false',
            'primary.containerSecurityContext.enabled=false',
            'volumePermissions.enabled=false',
            'shmVolume.enabled=false',
            'primary.extraVolumes=null',
            'primary.extraVolumeMounts=null'
        ]
    )
    k8s_resource(
        'postgresql',
        port_forwards=['5432:5432'],
        links=[
            link('postgresql://metaflow:metaflow@localhost:5432/metaflow', 'PostgreSQL Connection')
        ],
        labels=['postgresql'],
        resource_deps=components['postgresql'],
    )

#################################################
# ARGO WORKFLOWS
#################################################
if "argo-workflows" in enabled_components:
    helm_remote(
        'argo-workflows',
        repo_name='argo',
        repo_url='https://argoproj.github.io/argo-helm',
        set=[
            'server.extraArgs[0]=--auth-mode=server',
            'workflow.serviceAccount.create=true',
            'workflow.rbac.create=true',
            'server.livenessProbe.initialDelaySeconds=1',
            'server.readinessProbe.initialDelaySeconds=1',
            'server.resources.requests.memory=128Mi',
            'server.resources.requests.cpu=50m',
            'server.resources.limits.memory=256Mi',
            'server.resources.limits.cpu=100m',
            'controller.resources.requests.memory=128Mi',
            'controller.resources.requests.cpu=50m',
            'controller.resources.limits.memory=256Mi',
            'controller.resources.limits.cpu=100m'
        ]
    )

    k8s_resource(
        workload='argo-workflows-server',
        port_forwards=['2746:2746'],
        links=[
            link('http://localhost:2746', 'Argo Workflows UI')
        ],
        labels=['argo-workflows'],
        resource_deps=components['argo-workflows']
    )

    k8s_resource(
        workload='argo-workflows-workflow-controller',
        labels=['argo-workflows'],
        resource_deps=components['argo-workflows']
    )

    k8s_yaml(encode_yaml({
        'apiVersion': 'rbac.authorization.k8s.io/v1',
        'kind': 'Role',
        'metadata': {
            'name': 'argo-workflowtaskresults-role',
            'namespace': 'default'
        },
        'rules': [{
            'apiGroups': ['argoproj.io'],
            'resources': ['workflowtaskresults'],
            'verbs': ['create', 'patch', 'get', 'list']
        }]
    }))

    k8s_yaml(encode_yaml({
        'apiVersion': 'rbac.authorization.k8s.io/v1',
        'kind': 'RoleBinding',
        'metadata': {
            'name': 'default-argo-workflowtaskresults-binding',
            'namespace': 'default'
        },
        'subjects': [{
            'kind': 'ServiceAccount',
            'name': 'default',
            'namespace': 'default'
        }],
        'roleRef': {
            'kind': 'Role',
            'name': 'argo-workflowtaskresults-role',
            'apiGroup': 'rbac.authorization.k8s.io'
        }
    }))

#################################################
# ARGO EVENTS
#################################################
if "argo-events" in enabled_components:
    helm_remote(
        'argo-events',
        repo_name='argo',
        repo_url='https://argoproj.github.io/argo-helm',
        set=[
            'crds.install=true',
            'controller.metrics.enabled=true',
            'controller.livenessProbe.initialDelaySeconds=1',
            'controller.readinessProbe.initialDelaySeconds=1',
            'controller.resources.requests.memory=64Mi',
            'controller.resources.requests.cpu=25m',
            'controller.resources.limits.memory=128Mi',
            'controller.resources.limits.cpu=50m',
            'configs.jetstream.streamConfig.maxAge=72h',
            'configs.jetstream.streamConfig.replicas=1',
        ]
    )

    k8s_yaml(encode_yaml({
        'apiVersion': 'argoproj.io/v1alpha1',
        'kind': 'EventBus',
        'metadata': {
            'name': 'argo-events-bus',
            'namespace': 'default'
        },
        'spec': {
            'jetstream': {
                'version': 'latest',
                'replicas': 1,
                'containerTemplate': {
                    'resources': {
                        'limits': {
                            'cpu': '100m',
                            'memory': '128Mi'
                        },
                        'requests': {
                            'cpu': '100m',
                            'memory': '128Mi'
                        }
                    }
                }
            }
        }
    }))

    k8s_yaml(encode_yaml({
        'apiVersion': 'argoproj.io/v1alpha1',
        'kind': 'EventSource',
        'metadata': {
            'name': 'argo-events-webhook',
            'namespace': 'default'
        },
        'spec': {
            'eventBusName': 'argo-events-bus',
            'template': {
                'container': {
                    'resources': {
                        'requests': {
                            'cpu': '25m',
                            'memory': '50Mi'
                        },
                        'limits': {
                            'cpu': '25m',
                            'memory': '50Mi'
                        }
                    }
                }
            },
            'service': {
                'ports': [
                    {
                        'port': 12000,
                        'targetPort': 12000
                    }
                ]
            },
            'webhook': {
                'metaflow-event': {
                    'port': '12000',
                    'endpoint': '/metaflow-event',
                    'method': 'POST'
                }
            }
        }
    }))


    k8s_resource(
        'argo-events-controller-manager',
        labels=['argo-events'],
    )

    # k8s_resource(
	# 	"argo-events-webhook-eventsource-svc",
	# 	port_forwards=["12000:12000"],
	# )

#################################################
# METADATA SERVICE
#################################################
if "metadata-service" in enabled_components:
    helm_remote(
        'metaflow-service',
        repo_name='metaflow',
        repo_url='https://outerbounds.github.io/metaflow-tools',
        set=[
            'metadatadb.user=metaflow',
            'metadatadb.password=metaflow',
            'metadatadb.database=metaflow',
            'metadatadb.host=postgresql',
            'resources.requests.cpu=25m',
            'resources.requests.memory=64Mi',
            'resources.limits.cpu=50m',
            'resources.limits.memory=128Mi'
        ]
    )

    k8s_resource(
        'metaflow-service',
        port_forwards=['8080:8080'],
        links=[link('http://localhost:8080/ping', 'Ping Metaflow Service')],
        labels=['metaflow-service'],
        resource_deps=components['metadata-service']
    )

#################################################
# KUEUE
#################################################
if "kueue" in enabled_components:
    helm_remote(
        'kueue',
        repo_name='kueue',
        repo_url='oci://us-central1-docker.pkg.dev/k8s-staging-images/charts',
        version='v0.10.1',
        set=[
            'resources.requests.memory=64Mi',
            'resources.requests.cpu=25m',
            'resources.limits.memory=128Mi',
            'resources.limits.cpu=50m',
            'controllerManager.replicas=1',
            # 'webhookService.name=kueue-webhook-service',
            # 'webhookService.namespace=kueue-system',
            # 'admissionWebhooks.enabled=true'
        ]
    )

    k8s_resource(
        'kueue-controller-manager',
        labels=['kueue'],
		resource_deps=['postgresql']
    )



# if "jobsets" in enabled_components:
#     local_resource(
#         'fetch-jobset-manifests',
#         'curl -L https://github.com/kubernetes-sigs/jobset/releases/download/v0.7.2/manifests.yaml -o jobset-manifests.yaml',
#         deps=[],
#     )

#     # Apply resource constraints to the JobSet controller
#     k8s_yaml(blob(read_file('jobset-manifests.yaml')
#         .replace(
#             'resources:',
#             '''resources:
#             limits:
#               cpu: 50m
#               memory: 128Mi
#             requests:
#               cpu: 25m
#               memory: 64Mi'''
#         )),
#         resource_deps=['fetch-jobset-manifests'])

#     k8s_resource(
#         'jobset-controller-manager',
#         labels=['jobset'],
#         resource_deps=['kueue-controller-manager']
#     )
