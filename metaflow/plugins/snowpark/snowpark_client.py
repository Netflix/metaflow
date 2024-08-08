import sys
import tempfile

from .snowpark_exceptions import SnowflakeException
from .snowpark_service_spec import generate_spec_file

from metaflow.exception import MetaflowException


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
        autocommit: bool = True,
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

    def __check_existence_of_stage_and_compute_pool(
        self, db, schema, stage, compute_pool
    ):
        from snowflake.core.exceptions import NotFoundError

        # check if stage exists, will raise an error otherwise
        try:
            self.root.databases[db].schemas[schema].stages[stage].fetch()
        except NotFoundError:
            raise MetaflowException(
                "Stage *%s* does not exist or not authorized." % stage
            )

        # check if compute_pool exists, will raise an error otherwise
        try:
            self.root.compute_pools[compute_pool].fetch()
        except NotFoundError:
            raise MetaflowException(
                "Compute pool *%s* does not exist or not authorized." % compute_pool
            )

    def submit(self, name: str, spec, stage, compute_pool, external_integration):
        db = self.session.get_current_database()
        schema = self.session.get_current_schema()

        with tempfile.TemporaryDirectory() as temp_dir:
            snowpark_spec_file = tempfile.NamedTemporaryFile(
                dir=temp_dir, delete=False, suffix=".yaml"
            )
            generate_spec_file(spec, snowpark_spec_file.name, format="yaml")

            self.__check_existence_of_stage_and_compute_pool(
                db, schema, stage, compute_pool
            )

            # upload the spec file to stage
            result = self.session.file.put(snowpark_spec_file.name, "@%s" % stage)

            service_name = name.replace("-", "_")
            external_access = (
                "EXTERNAL_ACCESS_INTEGRATIONS=(%s) " % ",".join(external_integration)
                if external_integration
                else ""
            )

            # cannot pass 'is_job' parameter using the API, thus we need to use SQL directly..
            query = """
            EXECUTE JOB SERVICE IN COMPUTE POOL {compute_pool}
            NAME = {db}.{schema}.{service_name}
            {external_access}
            FROM @{stage} SPECIFICATION_FILE={specification_file}
            """.format(
                compute_pool=compute_pool,
                db=db,
                schema=schema,
                service_name=service_name,
                external_access=external_access,
                stage=stage,
                specification_file=result[0].target,
            )

            async_job = self.session.sql(query).collect(block=False)
            return async_job.query_id, service_name

    def terminate_job(self, service):
        service.delete()
