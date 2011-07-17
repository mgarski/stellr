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

import platform
import subprocess
import time
import unittest

import tornado.httpserver
import tornado.ioloop
import tornado.web

import stellr

HDR_CONTENT_TYPE = 'Content-Type'
HDR_JSON = 'application/json'

TEST_HOST = 'http://localhost:8080'

CLAUSES = [('q', 'test query'), ('sort', 'name asc')]

DOCUMENTS = [
        ['a', 1, ['a1', '1a']],
        ['b', 2, ['b2', '2b']]]

FIELDS = ['field1', 'field2', 'listField']

class StellrConnectionTest(unittest.TestCase):

    def test_blocking_connection(self):
        conn = stellr.BlockingConnection(TEST_HOST)
        query = stellr.QueryCommand(handler='/query')
        query.add_param('q', 'a')

        response = conn.execute(query)
        self.assertEquals(response['response']['q'], ['a'])

    def test_blocking_connection_timeout(self):
        # mac blocking calls not supported :(
        self.assertFalse(platform.mac_ver()[0],
            'Blocking connection timeouts are not supported on Mac')

        conn = stellr.BlockingConnection(TEST_HOST, timeout=2)
        query = stellr.QueryCommand(handler='/query')
        query.add_param('q', 'a')
        query.add_param('s', '3')

        success = False
        try:
            conn.execute(query)
            success = True
        except stellr.StellrError as e:
            self.assertTrue(e.timeout)

        self.assertFalse(success)

    def test_tornado_connection(self):
        conn = stellr.TornadoConnection(TEST_HOST)
        query = stellr.QueryCommand(handler='/query')
        query.add_param('q', 'a')
        conn.execute(query, self._handle_response)
        tornado.ioloop.IOLoop.instance().start()

    def test_tornado_connection_timeout(self):
        conn = stellr.TornadoConnection(TEST_HOST, timeout=2)
        query = stellr.QueryCommand(handler='/query')
        query.add_param('q', 'a')
        query.add_param('s', '3')
        conn.execute(query, self._handle_timeout)
        tornado.ioloop.IOLoop.instance().start()

    def _handle_response(self, response):
        self.assertFalse(response.error)
        self.assertEquals(response.body['response']['q'], ['a'])
        tornado.ioloop.IOLoop.instance().stop()

    def _handle_timeout(self, response):
        self.assertTrue(response.error.timeout)
        tornado.ioloop.IOLoop.instance().stop()