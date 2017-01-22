import falcon
import picks_analytics


api = application = falcon.API()

# storage_path = '/Users/shaunchaudhary/Documents/image_output'
#
# image_collection = falcon_tutorial_images.Collection(storage_path)
# image = falcon_tutorial_images.Item(storage_path)

picks_analytics = picks_analytics.Win_Ratio()
api.add_route('/win_ratio/{freq}/{head}', picks_analytics)
