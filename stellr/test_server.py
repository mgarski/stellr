#   Copyright 2011 Michael Garski (mgarski@mac.com)
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

import json
import time

import tornado.httpserver
import tornado.ioloop
import tornado.web

HDR_CONTENT_TYPE = 'Content-Type'
HDR_JSON = 'application/json'

PORT = 8080

class QueryHandler(tornado.web.RequestHandler):
    def post(self, *args, **kwargs):
        sleep = self.get_argument('s', 0)
        if sleep > 0:
            time.sleep(int(sleep))

        response = {'response': self.request.arguments }
        self.set_header(HDR_CONTENT_TYPE, HDR_JSON)
        self.write(json.dumps(response))

class UpdateHandler(tornado.web.RequestHandler):
    def post(self, *args, **kwargs):
        self.write(json.dumps(self.request.arguments))

application = tornado.web.Application([
    (r"/query", QueryHandler),
    (r"/update", UpdateHandler)
])

class TestServer():
    def start(self):
        http_server = tornado.httpserver.HTTPServer(application)
        http_server.listen(PORT)
        tornado.ioloop.IOLoop.instance().start()

if __name__ == "__main__":
    server = TestServer()
    server.start()
    print('server started')