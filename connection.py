import sqlalchemy
from sqlalchemy import create_engine, event
import os
import db


class AWSEngine:

    def __init__(self):
        self.engine_str = self.build_engine()

    @staticmethod
    def init_search_path(connection, conn_record):
        cursor = connection.cursor()
        try:
            cursor.execute('SET search_path TO new_db_schema;')
        finally:
            cursor.close()

    def build_engine(self):
        # os.environ['AWS_KEY']
        engine = create_engine(db.get_aws_cred())
        event.listen(engine, 'connect', self.init_search_path)

        meta = sqlalchemy.MetaData(engine, schema='schema')
        meta.reflect(engine, schema='schema')

        return engine
