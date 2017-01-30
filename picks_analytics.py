import falcon
import win_ratio
import weekly_review


class WinRatio(object):

    def on_get(self, req, resp, freq, head):
        resp.body = win_ratio.get_data_dfs(freq, head=head)
        resp.status = falcon.HTTP_200


class WeeklyRecord(object):

    def on_get(self, req, resp, week):
        resp.body = weekly_review.weekly_league_record(week)
        resp.status = falcon.HTTP_200


class WeeklyRecDistr(object):

    def on_get(self, req, resp, week):
        resp.body = weekly_review.week_rec_distr(week)
        resp.status = falcon.HTTP_200


class UserWeeklyRecords(object):

    def on_get(self, req, resp, week):
        resp.body = weekly_review.user_weekly_record(week)
        resp.status = falcon.HTTP_200
