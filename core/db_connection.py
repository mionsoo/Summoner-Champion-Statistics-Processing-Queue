from common.db import connect_sql_aurora, RDS_INSTANCE_TYPE, sql_execute


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