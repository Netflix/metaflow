import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from snowflake.connector import DictCursor
from snowflake.connector.converter_null import SnowflakeNoConverterToPython


import logging
import os


class SnowflakeAsyncClient(object):
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

    def submit(self, query):
        try:
            cursor = self.ctx.cursor()
            cursor.execute_async(query, _no_results=True)
            query_id = cursor.sfqid
            cursor.close()
            return query_id
        except ProgrammingError as e:
            print(f"Failed to execute query: {e}")
            return None

    def cancel(self, id):
        cursor = self.ctx.cursor()
        try:
            cursor.execute(r"SELECT SYSTEM$CANCEL_QUERY('id')")
            result = cursor.fetchall()
            print(len(result))
            print(result[0])
        finally:
            cursor.close()

    def status(self, id):
        cursor = self.ctx.cursor()
        return self.ctx.get_query_status(id)


if __name__ == "__main__":
    client = SnowflakeAsyncClient(
        user="SAVINOUTERBOUNDS", password="Thefray7", account="pfb48862"
    )

    _id = client.submit(
        "EXECUTE SERVICE IN COMPUTE POOL tutorial_compute_pool FROM @TUTORIAL_DB.DATA_SCHEMA.TUTORIAL_STAGE SPEC='my_job_spec.yaml';"
    )
    print(_id)
    print(client.status(_id))
