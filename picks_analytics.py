import falcon
import win_ratio


class Win_Ratio(object):

    def on_get(self, req, resp, freq, head):
        resp.body = win_ratio.get_data_dfs(freq, head=head)
        resp.status = falcon.HTTP_200

