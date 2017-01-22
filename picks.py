import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, event
import db
import json
import numpy


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


def get_picks(engine):

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

    return pd.read_sql(picks_q, engine)


def calc_counts(picks_df):

    picks_df['win_loss'] = picks_df['result'].apply(lambda x: 1 if x == 'win' else 0)

    for col, group in zip(['season', 'weekly', 'user'], ['team', ['team', 'week'], ['user', 'team']]):
        picks_df['{}_team_count'.format(col)] = picks_df.groupby(group)['team'].transform('count')
        picks_df['{}_team_wins'.format(col)] = picks_df.groupby(group)['win_loss'].transform(sum)

    return picks_df


class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)


def nested_json(df, head):

    output = {}
    if head == 'team':
        index = 'week'
    elif head == 'user':
        index = 'team'
    elif df.name == 'user':
        index = 'user'
    else:
        index = 'team'

    for name, group in df.groupby(head):
        group = group.set_index(index)
        group.index = group.index.astype(str)
        group.pop(head)
        output[name] = group.to_dict()

    return json.dumps(output, ensure_ascii=False, cls=MyEncoder)


def get_data_dfs(data_cut, head="team"):

    picks_df = get_picks(build_engine())
    picks_df = calc_counts(picks_df)

    count = "{}_team_count".format(data_cut)
    wins = "{}_team_wins".format(data_cut)
    win_pct = "{}_win_pct".format(data_cut)

    if data_cut == "season":
        cols = ["team", count, wins]
    elif data_cut == "weekly":
        cols = ["team", "week", count, "win_loss"]
    else:
        cols = ["user", "team", count, wins]

    df = picks_df[cols].drop_duplicates()
    df.name = data_cut

    if data_cut != "weekly":
        df[win_pct] = df[wins] / df[count]

    if data_cut == "season":
        df = df.sort_values([count, win_pct], ascending=False).set_index('team')
        return df.set_index('team').to_json()
    else:
        return nested_json(df, head)
