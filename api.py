import falcon

import falcon_tutorial_images


api = application = falcon.API()

storage_path = '/Users/shaunchaudhary/Documents/image_output'

image_collection = falcon_tutorial_images.Collection(storage_path)
image = falcon_tutorial_images.Item(storage_path)

api.add_route('/images', image_collection)
api.add_route('/images/{name}', image)