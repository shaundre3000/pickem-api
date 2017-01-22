import falcon
import picks_analytics


api = application = falcon.API()

picks_analytics = picks_analytics.Win_Ratio()
api.add_route('/win_ratio/{freq}/{head}', picks_analytics)
