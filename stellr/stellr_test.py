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

import time
import json
import threading
import unittest

import tornado.httpserver
import tornado.ioloop
import tornado.web

import stellr

HDR_CONTENT_TYPE = 'Content-Type'
HDR_JSON = 'application/json'

PRIMARY_PORT = 8080

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

class StellrHandler(tornado.web.RequestHandler):
    def get(self, *args, **kwargs):
        #TODO: create a stellr request to itself!
        #TODO: support a timeout
        self.write(json.dumps(self.request.arguments))


application = tornado.web.Application([
    (r"/query", QueryHandler),
])

class PrimaryApplication(threading.Thread):
    def run(self):
        http_server = tornado.httpserver.HTTPServer(application)
        http_server.listen(PRIMARY_PORT)
        tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    p = PrimaryApplication()
    p.start()
    print('server started')

    conn = stellr.BlockingConnection('http://localhost:' + str(PRIMARY_PORT))
    query = stellr.QueryCommand(handler='/query')
    query.add_param('q', 'a')

    #TODO: assert this
    print(conn.execute(query))

    conn = stellr.BlockingConnection('http://localhost:' + str(PRIMARY_PORT), timeout=5)
    query = stellr.QueryCommand(handler='/query')
    query.add_param('q', 'a')
    query.add_param('s', '10')
    try:
        conn.execute(query)
    except stellr.StellrError as e:
        #TODO: assert this
        print('Timeout: %s' % e.timeout)

    tornado.ioloop.IOLoop.instance().stop()



#
#class MockObj(object):
#    pass

#    try:
#        test = BlockingConnection('test')
#        print(test)
#    except StellrError:
#        print("BlockingConnection failed")
#
#
#    try:
#        test = TornadoConnection("test", "u", "p")
#        print(test)
#    except StellrError:
#        print("TornadoConnection failed")
#
#    q = QueryCommand()
#    q.add_param('q', 'test')
#    q.add_param('qf', 'one,two')
#    print(q._commands)
#    print(q.data)
#
#    a = UpdateCommand(commit_within=1000)
#    a.add_update([{'d': 'val1', 'e': 2},{'d': 'val2', 'e':[1,2,'3']}])
#    print(a.data)
#
#    m = MockObj()
#    m.a = '123'
#    m.b = [1, 2, 3]
#    a.add_update(m)
#    print(a.data)
#
#    conn = BlockingConnection('http://localhost:8983')
#    query = QueryCommand(handler='/solr/topic/select')
#    query.add_param('q', 'a')
#    try:
#        print(conn.execute(query))
#    except StellrError as e:
#        print(e.url + '\n')
#        print(e.message)
#
#    conn = BlockingConnection('http://localhost:8983',
#                              user='update', password='update!', timeout=1500)
#    update = UpdateCommand(handler='/solr/topic/update/json',
#                           commit=True)
#    update.add_update({'id': 'mikey', 'topicname': 'mikey', 'topicuri':'/mikey', 'topictype':'mikey'})
#    m = MockObj()
#    m.id = 'lynda'
#    m.topicname = 'lynda'
#    m.topicuri = '/lynda'
#    m.topictype = 'lynda'
#    update.add_update(m)
#
#    try:
#        print(conn.execute(update))
#    except StellrError as e:
#        print(e.url)
#        print(e.message)
#        print('Time out? ' + str(e.timeout))

#    conn = BlockingConnection('http://localhost:8983')
#    query = QueryCommand(handler='/solr/topic/select')
#    query.add_param('q', 'id:mikey id:lynda')
#    try:
#        print(conn.execute(query))
#    except StellrError as e:
#        print(e.url + '\n')
#        print(e.message)
#
#    import tornado.ioloop
#
#    def handle_request(response):
#        if response.error:
#            print "Error:", response.error.inner
#        else:
#            print response.body
#        tornado.ioloop.IOLoop.instance().stop()
#
#    conn = TornadoConnection('http://localhost:8983', timeout=0)
#    query = QueryCommand(handler='/solr/topic/select')
#    query.add_param('q', 'id:mikey id:lynda')
#    conn.execute(query, handle_request)
#    tornado.ioloop.IOLoop.instance().start()
