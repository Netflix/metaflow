import sys
import tempfile

from .snowpark_exceptions import SnowflakeException
from .snowpark_service_spec import generate_spec_file


class SnowparkClient(object):
    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        role: str,
        database: str,
        warehouse: str,
        schema: str,
        autocommit: bool,
    ):
        try:
            from snowflake.core import Root
            from snowflake.snowpark import Session

            from snowflake.connector.errors import DatabaseError
        except (NameError, ImportError, ModuleNotFoundError):
            raise SnowflakeException(
                "Could not import module 'snowflake'.\n\nInstall Snowflake "
                "Python package (https://pypi.org/project/snowflake/) first.\n"
                "You can install the module by executing - "
                "%s -m pip install snowflake\n"
                "or equivalent through your favorite Python package manager."
                % sys.executable
            )

        self.connection_parameters = {
            "account": account,
            "user": user,
            "password": password,
            "role": role,
            "warehouse": warehouse,
            "database": database,
            "schema": schema,
            "autocommit": autocommit,
        }

        try:
            self.session = Session.builder.configs(self.connection_parameters).create()
            self.root = Root(self.session)
        except DatabaseError as e:
            raise ConnectionError(e)

    def __del__(self):
        if hasattr(self, "session"):
            self.session.close()

    def submit(self, name: str, spec, stage, compute_pool, external_integration):
        db = self.session.get_current_database()
        schema = self.session.get_current_schema()

        with tempfile.TemporaryDirectory() as temp_dir:
            snowpark_spec_file = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False, suffix=".yaml"
            )
            generate_spec_file(spec, snowpark_spec_file.name, format="yaml")

            # check if stage exists, will raise an error otherwise
            self.root.databases[db].schemas[schema].stages[stage].fetch()

            # check if compute_pool exists, will raise an error otherwise
            self.root.compute_pools[compute_pool].fetch()

            # upload the spec file to stage
            result = self.session.file.put(snowpark_spec_file.name, f"@{stage}")

            service_name = name.replace("-", "_")
            external_access = (
                f"EXTERNAL_ACCESS_INTEGRATIONS=({external_integration}) "
                if external_integration
                else ""
            )

            # cannot pass 'is_job' parameter using the API, thus we need to use SQL directly..
            query = f"""
            EXECUTE JOB SERVICE IN COMPUTE POOL {compute_pool}
            NAME = {db}.{schema}.{service_name}
            {external_access}
            FROM @{stage} SPECIFICATION_FILE={result[0].target}
            """

            async_job = self.session.sql(query).collect(block=False)
            return async_job.query_id, service_name

    def terminate_job(self, service):
        service.suspend()
        service.delete()