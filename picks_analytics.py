import falcon
import picks


class Win_Ratio(object):

    def on_get(self, req, resp, freq, head):
        resp.body = picks.get_data_dfs(freq, head=head)
        resp.status = falcon.HTTP_200

