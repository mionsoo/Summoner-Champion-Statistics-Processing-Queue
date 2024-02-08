import traceback

from common.db import connect_sql_aurora, RDS_INSTANCE_TYPE, sql_execute, connect_sql_aurora_async


class DBConn:
    def __init__(self):
        self.connection = None

    def make_conn(self):
        self.connection = connect_sql_aurora(RDS_INSTANCE_TYPE.READ)

    def select(self, query):
        return sql_execute(query=query, conn=self.connection)

    def insert(self, query):
        sql_execute(query=query, conn=self.connection)
        self.connection.commit()

    def update(self, query):
        sql_execute(query=query, conn=self.connection)
        self.connection.commit()

    def close(self):
        self.connection.close()
        self.connection = None


class AsyncDBConn:
    def __init__(self):
        self.connection = None

    async def make_connection(self):
        self.connection = await connect_sql_aurora_async(RDS_INSTANCE_TYPE.READ)
        # self.connection = connect_sql_aurora(RDS_INSTANCE_TYPE.READ)

    async def select(self, query):
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(query)
                return cursor.fetchall()
        except Exception:
            print('sql select error: ', traceback.format_exc())

    async def insert(self, query):
        try:
            async with self.connection.cursor() as cursor:
                await cursor.execute(query)
            await self.connection.commit()
        except Exception:
            pass

    def update(self, query):
        sql_execute(query=query, conn=self.connection)
        self.connection.commit()

    def close(self):
        self.connection.close()
        self.connection = None