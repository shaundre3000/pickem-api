import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, event
import db


def init_search_path(connection, conn_record):
    cursor = connection.cursor()
    try:
        cursor.execute('SET search_path TO new_db_schema;')
    finally:
        cursor.close()


def build_engine():

    engine = create_engine(db.get_aws_cred())
    event.listen(engine, 'connect', init_search_path)

    meta = sqlalchemy.MetaData(engine, schema='schema')
    meta.reflect(engine, schema='schema')

    return engine


engine = build_engine()
picks_q = """
SELECT

    u.name as user,
    p.game_id,
    t.name as team,
    p.week,
    p.result

from picks p

LEFT JOIN users u on p.user_id = u.id

LEFT JOIN teams t on p.winner_id = t.id"""

picks_df = pd.read_sql(picks_q, engine)

