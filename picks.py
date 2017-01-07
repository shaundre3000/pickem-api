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


def get_data_dfs(picks_df, data_cut):

    count = '{}_team_count'.format(data_cut)
    wins = '{}_team_wins'.format(data_cut)
    win_pct = '{}_win_pct'.format(data_cut)

    if data_cut == 'season':
        cols = ['team', count, wins]
    elif data_cut == 'weekly':
        cols = ['team', 'week', count, 'win_loss']
    else:
        cols = ['user', 'team', count, wins]

    df = picks_df[cols].drop_duplicates()

    if data_cut != 'weekly':
        df[win_pct] = df[wins] / df[count]
        return df.sort_values([count, win_pct], ascending=False)
    else:
        return df.sort_values([count, 'win_loss'], ascending=False)


def main():

    engine = build_engine()
    picks_df = get_picks(engine)
    picks_df = calc_counts(picks_df)
    season_team_df = get_data_dfs(picks_df, 'season')
    weekly_team_df = get_data_dfs(picks_df, 'weekly')
    user_team_df = get_data_dfs(picks_df, 'user')
