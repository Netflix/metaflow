import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from snowflake.connector import DictCursor

class SnowflakeAsyncClient(object):

    def __init__(self, user, password, account, role="test_role",warehouse="TUTORIAL_WAREHOUSE", database="TUTORIAL_DB",  schema="TUTORIAL_DB.DATA_SCHEMA"):
        self.ctx = snowflake.connector.connect(
            user=user,
            password=password,
            role=role,
            account=account,
            warehouse=warehouse,
            database=database,
            schema=schema
        )
        self.ctx.autocommit(True)

    def __del__(self):
        self.ctx.close()

    def execute_query_async(self, query):
        try:
            cursor = self.ctx.cursor()
            cursor.execute("ALTER SESSION SET QUERY_TAG = 'ASYNC_QUERY'")
            cursor.execute(query, _no_results=True)
            query_id = cursor.sfqid
            cursor.close()
            return query_id
        except ProgrammingError as e:
            print(f"Failed to execute query: {e}")
            return None

    def check_query_status(self, query_id):
        try:
            cursor = self.ctx.cursor()
            cursor.execute(f"SHOW TRANSACTIONS LIKE '{query_id}'")
            result = cursor.fetchone()
            cursor.close()
            if result:
                return result[5]  # Status column in SHOW TRANSACTIONS result
            else:
                return "Query ID not found"
        except ProgrammingError as e:
            print(f"Failed to check query status: {e}")
            return None

if __name__ == "__main__":
    client = SnowflakeAsyncClient(user="SAVINOUTERBOUNDS", password="Thefray7", account="pfb48862")
    _id = client.execute_query_async("PUT file:///Users/savin/Downloads/SnowparkContainerServices-Tutorials/Tutorial-2/my_job_spec.yaml @TUTORIAL_DB.DATA_SCHEMA.TUTORIAL_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;")
    print(_id)
    _id = client.execute_query_async("EXECUTE SERVICE IN COMPUTE POOL tutorial_compute_pool FROM @tutorial_stage SPEC='my_job_spec.yaml';")
    print(_id)
    print(client.check_query_status(_id))