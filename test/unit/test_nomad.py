import pytest
import json

from metaflow.decorators import StepDecorator, flow_decorators
from metaflow.flowspec import FlowSpec
from metaflow.plugins.nomad.nomad_decorator import NomadDecorator, NomadException
from metaflow.plugins.resources_decorator import ResourcesDecorator
from metaflow.plugins.nomad.nomad import NomadClient # For testing _generate_job_spec

# Mock Metaflow configurations for testing
# These would normally come from metaflow_config.py
MOCK_NOMAD_DEFAULT_REGION = "global-mock"
MOCK_NOMAD_DEFAULT_DATACENTERS = ["dc1-mock"]
MOCK_NOMAD_DEFAULT_IMAGE = "python:3.9-slim-mock"
MOCK_METAFLOW_NOMAD_CPU = "1500"
MOCK_METAFLOW_NOMAD_MEMORY = "3072"
MOCK_METAFLOW_NOMAD_DISK = "5120"

# Helper to create a mock flow and step context for decorator testing
def make_mock_flow_step(decos):
    class MockFlow(FlowSpec):
        @classmethod
        def get_top_level_options(cls): # Needed by environment.executable
            return {}

        _flow_decorators = flow_decorators() # Needed by environment.executable
        script_name = "mock_flow.py" # Needed by environment.executable

        @step
        def mock_step(self):
            pass

    flow = MockFlow()
    step = flow.mock_step

    # Initialize decorators
    # This is a simplified version of how Metaflow runtime initializes decorators
    # environment and flow_datastore are often needed by step_init
    from metaflow.metaflow_environment import MetaflowEnvironment
    from metaflow.datastore.local_storage import LocalStorage

    mock_logger = lambda *args, **kwargs: None # Simple mock logger
    mock_environment = MetaflowEnvironment(flow)
    # Mock datastore needs a sysroot
    LocalStorage.datastore_root = "/tmp/metaflow_test_ds" # Dummy path
    mock_flow_datastore = LocalStorage(flow.name)


    initialized_decos = []
    for deco_cls, attrs in decos:
        decorator_instance = deco_cls()
        decorator_instance.attributes = decorator_instance.defaults.copy()
        if attrs: # Apply provided attributes
            decorator_instance.attributes.update(attrs)

        # Call init and step_init if they exist
        if hasattr(decorator_instance, 'init'):
             decorator_instance.init() # Call init first
        if hasattr(decorator_instance, 'step_init'):
            decorator_instance.step_init(
                flow=flow,
                graph=flow._graph, # graph might be needed
                step=step.__name__,
                decos=[d[0]() for d in decos], # Pass uninitialized decorator instances
                environment=mock_environment,
                flow_datastore=mock_flow_datastore,
                logger=mock_logger
            )
        initialized_decos.append(decorator_instance)
    return flow, step, initialized_decos


class TestNomadDecorator:
    @pytest.fixture(autouse=True)
    def-monkeypatch_nomad_configs(self, monkeypatch):
        monkeypatch.setattr("metaflow.plugins.nomad.nomad_decorator.NOMAD_DEFAULT_REGION", MOCK_NOMAD_DEFAULT_REGION)
        monkeypatch.setattr("metaflow.plugins.nomad.nomad_decorator.NOMAD_DEFAULT_DATACENTERS", MOCK_NOMAD_DEFAULT_DATACENTERS)
        monkeypatch.setattr("metaflow.plugins.nomad.nomad_decorator.NOMAD_DEFAULT_IMAGE", MOCK_NOMAD_DEFAULT_IMAGE)
        monkeypatch.setattr("metaflow.plugins.nomad.nomad_decorator.METAFLOW_NOMAD_CPU", MOCK_METAFLOW_NOMAD_CPU)
        monkeypatch.setattr("metaflow.plugins.nomad.nomad_decorator.METAFLOW_NOMAD_MEMORY", MOCK_METAFLOW_NOMAD_MEMORY)
        monkeypatch.setattr("metaflow.plugins.nomad.nomad_decorator.METAFLOW_NOMAD_DISK", MOCK_METAFLOW_NOMAD_DISK)
        # Mock datastore for step_init check
        monkeypatch.setattr("metaflow.plugins.nomad.nomad_decorator.NomadException", NomadException)


    def test_decorator_defaults(self):
        # Test that decorator initializes with defaults from mock_config
        _, _, decos = make_mock_flow_step([ (NomadDecorator, {}) ])
        nomad_deco = decos[0]
        assert nomad_deco.attributes["region"] == MOCK_NOMAD_DEFAULT_REGION
        assert nomad_deco.attributes["datacenters"] == MOCK_NOMAD_DEFAULT_DATACENTERS
        assert nomad_deco.attributes["image"] == MOCK_NOMAD_DEFAULT_IMAGE
        assert nomad_deco.attributes["cpu"] == MOCK_METAFLOW_NOMAD_CPU
        assert nomad_deco.attributes["memory"] == MOCK_METAFLOW_NOMAD_MEMORY
        assert nomad_deco.attributes["disk"] == MOCK_METAFLOW_NOMAD_DISK
        assert nomad_deco.attributes["constraints"] is None

    def test_decorator_custom_values(self):
        custom_attrs = {
            "region": "us-east-1",
            "datacenters": ["dc-custom"],
            "image": "custom/image:latest",
            "cpu": "2000",
            "memory": "8192",
            "disk": "20480",
            "constraints": [{"attribute": "${attr.kernel.name}", "value": "linux"}]
        }
        _, _, decos = make_mock_flow_step([ (NomadDecorator, custom_attrs) ])
        nomad_deco = decos[0]
        assert nomad_deco.attributes["region"] == "us-east-1"
        assert nomad_deco.attributes["datacenters"] == ["dc-custom"]
        assert nomad_deco.attributes["image"] == "custom/image:latest"
        assert nomad_deco.attributes["cpu"] == "2000"
        assert nomad_deco.attributes["memory"] == "8192"
        assert nomad_deco.attributes["disk"] == "20480"
        assert nomad_deco.attributes["constraints"] == [{"attribute": "${attr.kernel.name}", "value": "linux"}]

    def test_decorator_with_resources(self):
        # @nomad takes precedence if specified, else @resources, else nomad defaults
        nomad_attrs = {"cpu": "3000", "image": "nomad/image"} # Nomad specifies CPU
        resources_attrs = {"cpu": "500", "memory": "1024", "disk": "1000"} # Resources specifies all

        _, _, decos = make_mock_flow_step([
            (ResourcesDecorator, resources_attrs),
            (NomadDecorator, nomad_attrs)
        ])
        nomad_deco = next(d for d in decos if isinstance(d, NomadDecorator))

        assert nomad_deco.attributes["image"] == "nomad/image" # From @nomad
        assert nomad_deco.attributes["cpu"] == "3000" # From @nomad (takes precedence, or max if logic is max)
                                                      # Current logic in decorator takes max.
        assert nomad_deco.attributes["memory"] == "1024" # From @resources
        assert nomad_deco.attributes["disk"] == "1000" # From @resources

    def test_decorator_resources_override_nomad_defaults(self):
        # @resources overrides @nomad's *default* values (those from METAFLOW_NOMAD_*)
        resources_attrs = {"cpu": "600", "memory": "2048", "disk": "2000"}
        _, _, decos = make_mock_flow_step([
            (ResourcesDecorator, resources_attrs),
            (NomadDecorator, {}) # Nomad decorator with no specific overrides
        ])
        nomad_deco = next(d for d in decos if isinstance(d, NomadDecorator))

        assert nomad_deco.attributes["cpu"] == "600" # From @resources
        assert nomad_deco.attributes["memory"] == "2048" # From @resources
        assert nomad_deco.attributes["disk"] == "2000" # From @resources
        assert nomad_deco.attributes["image"] == MOCK_NOMAD_DEFAULT_IMAGE # From nomad default config


    def test_decorator_invalid_resources(self):
        with pytest.raises(NomadException):
            make_mock_flow_step([(NomadDecorator, {"cpu": "-100"})])
        with pytest.raises(NomadException):
            make_mock_flow_step([(NomadDecorator, {"memory": "0"})])
        with pytest.raises(NomadException):
            make_mock_flow_step([(NomadDecorator, {"disk": "abc"})])

    def test_decorator_invalid_constraints_type(self):
        with pytest.raises(NomadException):
            make_mock_flow_step([(NomadDecorator, {"constraints": "not-a-list"})])
        with pytest.raises(NomadException):
            make_mock_flow_step([(NomadDecorator, {"constraints": ["not-a-dict"]})])

    # TODO: Test runtime_step_cli argument formatting

class TestNomadClientJobSpec:
    @pytest.fixture
    def nomad_client(self):
        # Mock NomadClient to avoid actual Nomad connection for spec generation
        # We only need to test _generate_job_spec which doesn't make API calls
        class MockNomadLibClient: # Mock for the `nomad.Nomad()` instance
            pass

        client = NomadClient(nomad_address="http://mock-nomad:4646")
        client.client = MockNomadLibClient() # Replace real client with mock
        return client

    def test_generate_job_spec_basic(self, nomad_client):
        spec = nomad_client._generate_job_spec(
            job_name="testflow-run1-step1-task1",
            task_name="mf-task-step1",
            image="test/image:latest",
            command_executable="/usr/bin/python",
            command_args=["flow.py", "step", "step1"],
            cpu="1000", memory="512", disk="256",
            datacenters=["dc1-test"], region="test-region", nomad_namespace="test-namespace",
            env_vars={"MY_VAR": "value1"},
            code_package_url="s3://bucket/path/to/package.tgz"
        )
        job = spec["Job"]
        assert job["ID"].startswith("mf-testflow-run1-step1-task1-")
        assert job["Name"] == "testflow-run1-step1-task1"
        assert job["Type"] == "batch"
        assert job["Datacenters"] == ["dc1-test"]
        assert job["Region"] == "test-region"
        assert job["Namespace"] == "test-namespace"

        tg = job["TaskGroups"][0]
        assert tg["Name"] == "tg-mf-task-step1"
        assert tg["Count"] == 1
        assert tg["EphemeralDisk"]["SizeMB"] == 256

        task = tg["Tasks"][0]
        assert task["Name"] == "mf-task-step1"
        assert task["Driver"] == "docker"
        assert task["Config"]["image"] == "test/image:latest"
        assert task["Config"]["command"] == "/usr/bin/python"
        assert task["Config"]["args"] == ["flow.py", "step", "step1"]
        assert task["Env"]["MY_VAR"] == "value1"
        assert task["Resources"]["CPU"] == 1000
        assert task["Resources"]["MemoryMB"] == 512
        # DiskMB at task level is not standard for Docker driver, it's at group level EphemeralDisk
        assert "DiskMB" not in task["Resources"]

        assert len(task["Artifacts"]) == 1
        assert task["Artifacts"][0]["GetterSource"] == "s3://bucket/path/to/package.tgz"
        assert task["Artifacts"][0]["RelativeDest"] == "local/mf-task-step1/"

    def test_generate_job_spec_with_constraints(self, nomad_client):
        constraints = [{"attribute": "${attr.kernel.name}", "operator": "=", "value": "linux"}]
        spec = nomad_client._generate_job_spec(
            job_name="constrained-job", task_name="mf-task-cstep",
            image="img", command_executable="py", command_args=["f.py"],
            cpu="100", memory="100", disk="100",
            datacenters=["dc1"], region="reg1", nomad_namespace="ns1",
            constraints=constraints
        )
        assert spec["Job"]["Constraints"] == constraints

    def test_generate_job_spec_no_disk(self, nomad_client):
        spec = nomad_client._generate_job_spec(
            job_name="nodisk-job", task_name="mf-task-ndstep",
            image="img", command_executable="py", command_args=["f.py"],
            cpu="100", memory="100", disk=None, # No disk specified
            datacenters=["dc1"], region="reg1", nomad_namespace="ns1"
        )
        tg = spec["Job"]["TaskGroups"][0]
        assert tg["EphemeralDisk"]["SizeMB"] == 1024 # Default fallback

    # TODO: Add more tests for _generate_job_spec, e.g. different env_vars, no code_package_url

# TODO: Add tests for NomadClient API interaction methods (launch_job, wait, logs, stop, list)
# These will require mocking the `python-nomad` library's client calls and responses.
# Example:
# from unittest.mock import MagicMock
# def test_launch_job_success(nomad_client_with_mocked_api):
#     nomad_client_with_mocked_api.client.job.register_job = MagicMock(return_value={"EvalID": "eval123"})
#     result = nomad_client_with_mocked_api.launch_job(...)
#     assert result["eval_id"] == "eval123"
#     nomad_client_with_mocked_api.client.job.register_job.assert_called_once()

# To run these tests:
# Ensure metaflow is installed in editable mode or python path includes the project root.
# `python -m pytest test/unit/test_nomad.py`
# May need to `pip install pytest`
# Ensure `python-nomad` is installed for `NomadClient` import, though it's mocked for spec generation.
# The `make_mock_flow_step` might need access to `metaflow_config` for some environment/datastore setups.
# Consider adding `from metaflow import metaflow_config` and monkeypatching its values directly
# if the decorator/client relies on them beyond what's passed or monkeypatched at module level.
# For `environment.executable(step_name)` to work without full flow setup, it might need more mocks.
# `LocalDatastore.datastore_root` is set for `LocalStorage` initialization.
# `FlowSpec._graph` is used; simple graph is fine for these unit tests.
# `FlowSpec.script_name` is added.
# `FlowSpec.get_top_level_options` is added.
# `FlowSpec._flow_decorators` is added.

# A fixture for a client with a fully mocked python-nomad Nomad() instance
@pytest.fixture
def nomad_client_with_mocked_api(monkeypatch):
    # Mock the entire python-nomad library instance
    mock_nomad_lib_instance = MagicMock()

    # Configure return values for specific mocked calls if needed for more complex tests
    # e.g., mock_nomad_lib_instance.job.register_job.return_value = {"EvalID": "mock-eval-id"}
    # e.g., mock_nomad_lib_instance.allocation.get_allocation.return_value = {"ClientStatus": "complete", ...}

    # Patch the nomad.Nomad class to return our mock instance
    monkeypatch.setattr("nomad.Nomad", lambda *args, **kwargs: mock_nomad_lib_instance)

    # Now, when NomadClient instantiates nomad.Nomad, it gets our MagicMock
    client = NomadClient(nomad_address="http://mock-nomad:4646") # Address doesn't matter now
    client.client = mock_nomad_lib_instance # Ensure it's using the top-level mock
    return client

# Need to import MagicMock for the above fixture
from unittest.mock import MagicMock
from metaflow import step # for @step in mock flow
