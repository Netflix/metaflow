import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from snowflake.connector import DictCursor
from snowflake.connector.converter_null import SnowflakeNoConverterToPython

from metaflow.exception import MetaflowException

from collections import defaultdict
import json


# TODO: Error Handling!

class SnowparkException(MetaflowException):
    headline = "Snowpark error"

class SnowflakeException(MetaflowException):
    headline = "Snowflake error"


class SnowparkKilledException(MetaflowException):
    headline = "Snowpark job killed"


class SnowparkJob(object):
    def __init__(
        self,
        cmd,
        env={},
        image="/tutorial_db/data_schema/tutorial_repository/test-image:latest",
    ):
        # TODO: Reuse client across Snowpark Jobs since they are a bit expensive
        self._client = SnowparkAsyncClient(
            user="SAVINOUTERBOUNDS", password="Thefray7", account="pfb48862"
        )
        self._spec = (
            SnowparkJobSpec()
            .container(Container(name="main", image=image).command(cmd).envs(env))
            .log_exporters(log_level="INFO")  # TODO: Comment this out!
            .to_yaml()
        )
        self._id = None
        self._status = {"status": "UNKNOWN"}

    def submit(self, compute_pool):
        self._id = self._client.submit(self._spec, compute_pool)
        return self._id

    @property
    def query_id(self):
        return self._id

    @property
    def spec(self):
        return self._spec

    @property
    def status(self):
        if self._status["status"] not in ["FAILED", "DONE"]:
            self._status = self._client.status(self.query_id)
        return self._status

    @property
    def is_running(self):
        # undocumented READY state also maps to running
        return self.status["status"] in ["PENDING", "READY"]

    @property
    def has_failed(self):
        return self.status["status"] == "FAILED"

    @property
    def has_succeeded(self):
        return self.status["status"] == "DONE"

    @property
    def has_finished(self):
        return self.has_succeeded or self.has_failed

    def cancel(self):
        if self.query_id is not None:
            self._client.cancel(self.query_id)


class SnowparkJobSpec(object):
    # https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference

    def __init__(self):
        tree = lambda: defaultdict(tree)
        self.payload = tree()

    def container(self, container):
        if "containers" not in self.payload:
            self.payload["containers"] = []
        self.payload["containers"].append(container.to_json())
        return self

    def volume(self, volume):
        # TODO: Implement before shipping!
        return self

    # https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-services#label-snowpark-containers-working-with-services-local-logs
    def log_exporters(self, log_level="NONE"):
        allowed_values = ["INFO", "ERROR", "NONE"]
        if log_level not in allowed_values:
            raise ValueError(
                f"Invalid value for log_exporters: {log_level}. Must be one of {allowed_values}."
            )
        self.payload["logExporters"]["eventTableConfig"]["logLevel"] = log_level
        return self

    def to_json(self):
        return self.payload

    def to_yaml(self):
        def dict_to_yaml(data, indent=0):
            yaml = ""
            space = "  " * indent
            if isinstance(data, dict):
                for key, value in data.items():
                    yaml += f"{space}{key}:"
                    if isinstance(value, (dict, list)):
                        yaml += f"\n{dict_to_yaml(value, indent + 1)}"
                    else:
                        yaml += f" {value}\n"
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, (dict, list)):
                        yaml += f"{space}-\n{dict_to_yaml(item, indent + 1)}"
                    else:
                        yaml += f"{space}- {item}\n"
            return yaml

        return dict_to_yaml({"spec": self.payload})

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class Container(object):
    # https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference#spec-containers-field-required

    def __init__(self, name, image):
        tree = lambda: defaultdict(tree)
        self.payload = tree()
        self.payload["name"] = name
        self.payload["image"] = image

    # TODO: Fix this!
    def command(self, command):
        self.payload["command"] = command
        return self

    def args(self, args):
        self.payload["args"] = args
        return self

    def env(self, key, value):
        if "env" not in self.payload:
            self.payload["env"] = {}
        self.payload["env"][key] = str(value)
        return self

    def envs(self, envs):
        for key, value in envs.items():
            self.env(key, value)
        return self

    def resources(self, resources):
        # TODO
        return self

    def volume_mount(self, volume_mount):
        # TODO
        return self

    def secret(self, secret):
        # TODO
        return self

    def to_json(self):
        return self.payload

    def __str__(self):
        return json.dumps(self.payload, indent=4)


class SnowparkAsyncClient(object):
    def __init__(
        self,
        user,
        password,
        account,
        role="test_role",
        warehouse="TUTORIAL_WAREHOUSE",
        database="TUTORIAL_DB",
        schema="DATA_SCHEMA",
    ):
        self.ctx = snowflake.connector.connect(
            user=user,
            password=password,
            role=role,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema,
            converter_class=SnowflakeNoConverterToPython,
        )
        self.ctx.autocommit(True)

    def __del__(self):
        self.ctx.close()

    def submit(self, spec, compute_pool):
        query = f"EXECUTE SERVICE IN COMPUTE POOL {compute_pool} FROM SPECIFICATION $${spec}$$ EXTERNAL_ACCESS_INTEGRATIONS = ('ALL_ACCESS_INTEGRATION1');"
        try:
            cursor = self.ctx.cursor()
            cursor.execute_async(query, _no_results=True)
            query_id = cursor.sfqid
            cursor.close()
            return query_id
        except ProgrammingError as e:
            raise SnowflakeException(f"Failed to execute query: {e}")

    def cancel(self, query_id):
        cursor = self.ctx.cursor()
        try:
            cursor.execute(f"SELECT SYSTEM$CANCEL_JOB('{query_id}')")
            result = cursor.fetchall()
            cursor.close()
            print(len(result))
            print(result[0])
        finally:
            cursor.close()

    def status(self, query_id):
        cursor = self.ctx.cursor()
        try:
            cursor.execute(f"SELECT SYSTEM$GET_JOB_STATUS('{query_id}')")
            result = cursor.fetchall()
            cursor.close()
            # currently supported status values include PENDING, FAILED, DONE, and 
            # UNKNOWN, per Snowflake documentation
            # https://docs.snowflake.com/en/sql-reference/functions/system_get_job_status
            return json.loads(result[0][0])[0]
        except ProgrammingError as e:
            # likely that job hasn't yet registered with the Snowpark Container Service
            # but it may very well never
            query_status = self.ctx.get_query_status(query_id)
            if self.ctx.is_an_error(self.ctx.get_query_status(query_id)):
                try:
                    cursor.get_results_from_sfqid(query_id)
                except Exception as e:
                    return {"status": "FAILED", "message": f"{e}", "transient": False}
            else:
                return {"status": "UNKNOWN"}
        finally:
            cursor.close()