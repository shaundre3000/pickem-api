import falcon
import picks_analytics
from auth import AuthMiddleware


# middleware=[AuthMiddleware()]
api = application = falcon.API()

picks_analytics = picks_analytics.Win_Ratio()
api.add_route('/win_ratio/{freq}/{head}', picks_analytics)
