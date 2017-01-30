from connection import AWSEngine
import pandas as pd
import numpy as np


def send_html_email(email_body, subject, sender, recipient, cc=None):
    """ Send an html email using smtp server. Only call this when using email tracebacks.
    See models>allocators/teamView.py or
    http://code.activestate.com/recipes/442459-email-pretty-tracebacks-to-yourself-or-someone-you/
    for an example of how to do so

    email_body: html input for your message
    subject: subject line as string
    sender: sender email address as string
    recipient: semicolon delimited string of recipients
    """

    import smtplib
    from weekly_review.mime.multipart import MIMEMultipart
    from weekly_review.mime.text import MIMEText

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = recipient
    if cc:
        msg['CC'] = cc

    part = MIMEText(email_body, 'html')

    msg.attach(part)

    server = smtplib.SMTP('smtp.gmail.com:587')
    server.ehlo()
    server.starttls()
    server.login('shaun.chaudhary@gmail.com', 'pkhmihzcpkrlazvp')
    server.sendmail(sender, recipient.split(';'), msg.as_string())
    server.close()


def get_picks_data():

    picks_q = """
    SELECT

        u.id as user,
        p.game_id,
        t.id as team,
        p.week,
        p.result

    from picks p

    LEFT JOIN users u on p.user_id = u.id

    LEFT JOIN teams t on p.winner_id = t.id"""

    engine = AWSEngine()

    return pd.read_sql(picks_q, engine.engine_str)


def weekly_league_record(week):

    picks_df = get_picks_data()

    outcome_cols = ['win', 'loss', 'push']
    for col in outcome_cols:
        picks_df[col] = picks_df['result'].apply(lambda result: 1 if result == col else 0)
        picks_df['weekly_{}'.format(col)] = picks_df.groupby('week')[col].transform(sum)
        picks_df['user_{}'.format(col)] = picks_df.groupby(['user', 'week'])[col].transform(sum)

    weekly_league_record = picks_df[['week'] + ['weekly_{}'.format(col) for col in outcome_cols]].drop_duplicates()
    weekly_league_record['weekly_points'] = weekly_league_record['weekly_win'] + (weekly_league_record['weekly_push'] * .5)
    weekly_league_record.sort_values('week', inplace=True)
    weekly_league_record.columns = ['Week', 'Wins', 'Loss', 'Push', 'Points']
    weekly_league_record['Points'] = weekly_league_record['Points'].apply(lambda x: np.nan if x == 0 else x)
    weekly_league_record.set_index('Week', inplace=True)

    if week == 'all':
        return weekly_league_record.dropna().to_json()
    else:
        return weekly_league_record[weekly_league_record['Week'] == week].dropna().to_json()


def user_weekly_record(week, distr=False):

    picks_df = get_picks_data()

    outcome_cols = ['win', 'loss', 'push']
    for col in outcome_cols:
        picks_df[col] = picks_df['result'].apply(lambda result: 1 if result == col else 0)
        picks_df['weekly_{}'.format(col)] = picks_df.groupby('week')[col].transform(sum)
        picks_df['user_{}'.format(col)] = picks_df.groupby(['user', 'week'])[col].transform(sum)
    user_weekly_record = picks_df[['user', 'week'] + ['user_{}'.format(col) for col in outcome_cols]].drop_duplicates()
    rec_format = lambda row: '{}-{}-{}'.format(row['user_win'], row['user_loss'], row['user_push'])
    user_weekly_record['weekly_record'] = user_weekly_record.apply(rec_format, axis=1)
    user_weekly_record['weekly_points'] = user_weekly_record['user_win'] + (user_weekly_record['user_push'] * .5)
    user_weekly_record = user_weekly_record[['user', 'week', 'weekly_record', 'weekly_points']]
    user_weekly_record['record_count'] = user_weekly_record.groupby(['weekly_record', 'week'])['user'].transform('count')

    if distr is True:
        return user_weekly_record
    else:
        if week == 'all':
            return user_weekly_record.set_index('User_ID').to_json()
        else:
            current_week = user_weekly_record[user_weekly_record['week'] == week].drop('record_count', axis=1)\
                .sort_values('weekly_points', ascending=False)
            current_week.drop('week', axis=1, inplace=True)
            current_week.columns = ['User_ID', 'Record', 'Points']
            return current_week.set_index('User_ID').to_json()


def week_rec_distr(week):

    current_week = user_weekly_record(week, distr=True)

    current_week_rec_dist = current_week[['week', 'weekly_record', 'record_count', 'weekly_points']]\
        .drop_duplicates().sort_values('weekly_points', ascending=False)
    current_week_rec_dist.set_index('week', inplace=True)
    current_week_rec_dist.columns = ['Record', 'Count', 'Points']

    if week == 'all':
        return current_week_rec_dist.to_json()
    else:
        return current_week_rec_dist[current_week_rec_dist['week'] == week].to_json()


def weekly_email(week):

    intro = """Hola from Costa Rica!\n
    Congrats to Neal Dennison for the final 5-0 of the year!  Updated <a href="http://ancient-wildwood-19051.herokuapp.com/standings">standings</a> are available on the website as is the <a href="http://ancient-wildwood-19051.herokuapp.com/distribution">picks distribution</a> for week 16.\n"""

    # picks_email = util.EmailBody(msg=intro)
    # picks_email.add_df_html(weekly_league_record, msg='League Record', underline=True, bold=True, index=False)
    # picks_email.add_df_html(current_week, msg='Individual Records', underline=True, bold=True, index=False)
    # picks_email.add_df_html(current_week_rec_dist, msg='Record Distributions', underline=True, bold=True, index=False)

    outro = """
    <a href="http://ancient-wildwood-19051.herokuapp.com/picks">Week 17 spreads</a> are up.\n
    Best,
    Shaun"""

    emails = [
        'shaun.chaudhary@gmail.com',
        'alexburness@gmail.com',
        'mosen.nu@gmail.com',
        'cjkreider@gmail.com',
        'lifeofryan@gmail.com',
        'stoneyy222@gmail.com',
        'joseph.dellafera@gmail.com',
        'oleary.will@gmail.com',
        'a.legaye@gmail.com',
        'shawn.israilov@gmail.com',
        'aj.miller91@gmail.com',
        'dylanalevy@gmail.com',
        'kbathija1@gmail.com',
        'varun8d@gmail.com',
        'rsahai91@gmail.com',
        'bryan.ashley@bashley.net',
        'jwdetmer@gmail.com',
        'wperge@gmail.com',
        'dkafaf391@gmail.com',
        'djh063@gmail.com',
        'nealdennison@gmail.com',
        'brett.graffy@gmail.com',
        'jpeters125@gmail.com',
        'josephiantosca@gmail.com',
        'sarna.brandon@gmail.com', ]

    # picks_email.add_msg(msg=outro)
    # send_html_email(picks_email.html, 'Week 16 Results and Week 17 Spreads', 'shaun.chaudhary@gmail.com', '; '.join(emails))


