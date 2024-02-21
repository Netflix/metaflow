import snowflake.connector
from snowflake.connector.errors import ProgrammingError

class SnowflakeAsyncClient(object):

    def __init__(self, user, password, account):
        self.ctx = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
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
	SnowflakeAsyncClient(user="SAVINOUTERBOUNDS", password="Thefray7.", account="BDHLWAK.MKB21361")