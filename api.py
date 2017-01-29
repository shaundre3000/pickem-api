import falcon
import picks_analytics


# middleware=[AuthMiddleware()]
api = application = falcon.API()

api.add_route('/win_ratio/{freq}/{head}', picks_analytics.WinRatio())
api.add_route('/weekly_record/{week}', picks_analytics.WeeklyRecord())
api.add_route('/weekly_record_distr/{week}', picks_analytics.WeeklyRecDistr())