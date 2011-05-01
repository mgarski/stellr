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

class MockObject(object):
    def __init__(self, field1, field2, listField):
        self.field1 = field1
        self.field2 = field2
        self.listField = listField

class StellrCommandTest(unittest.TestCase):

    def test_query_command(self):
        q = stellr.QueryCommand(handler='/solr/test/search/')
        self.assertEqual(q.handler, '/solr/test/search?wt=json')

        self._add_query_params(q, CLAUSES)
        self.assertEqual(q._commands, CLAUSES)

        self.assertEqual(q.data, 'q=test+query&sort=name+asc')

        q.clear_command()
        self.assertEqual(len(q._commands), 0)

    def test_update(self):
        u = stellr.UpdateCommand(commit_within=60000)
        self.assertEqual(u.handler, '/update/json?wt=json')

        a = MockObject(DOCUMENTS[0][0], DOCUMENTS[0][1], DOCUMENTS[0][2])
        u.add_update(a)

        b = dict()
        for i, field in enumerate(FIELDS):
            b[field] = DOCUMENTS[1][i]
        u.add_update(b)

        self.assertEqual(len(u._commands), 2)
        for i, command in enumerate(u._commands):
            self.assertEqual(command['commitWithin'], 60000)
            self.assertTrue('add' in command)
            self.assertTrue('doc' in command['add'])
            for field, value in command['add']['doc'].iteritems():
                field_ord = FIELDS.index(field)
                self.assertEqual(DOCUMENTS[i][field_ord], value)

    def test_delete(self):
        u = stellr.UpdateCommand()
        u.add_delete_by_id(0)
        u.add_delete_by_id([1, 2])
        self.assertTrue(len(u._commands), 3)
        for i, delete in enumerate(u._commands):
            self.assertEquals(delete['delete']['id'], i)

        u.clear_command()
        u.add_delete_by_query('field1:value0')
        u.add_delete_by_query(['field1:value1', 'field1:value2'])
        self.assertTrue(len(u._commands), 3)
        for i, delete in enumerate(u._commands):
            self.assertEquals(delete['delete']['query'],
                              'field1:value' + str(i))

    def test_commit(self):
        u = stellr.UpdateCommand(commit=True)
        self.assertEqual(u.handler, '/update/json?wt=json&commit=true')

        u.add_commit()
        self.assertEqual(len(u._commands), 1)
        self.assertTrue('commit' in u._commands[0])
        self.assertEqual(len(u._commands[0]['commit']), 0)

    def test_optimize(self):
        u = stellr.UpdateCommand()

        u.add_optimize()
        self.assertEqual(len(u._commands), 1)
        self.assertTrue('optimize' in u._commands[0])
        self.assertFalse(u._commands[0]['optimize']['waitFlush'])
        self.assertFalse(u._commands[0]['optimize']['waitSearcher'])

        u.clear_command()
        u.add_optimize(wait_flush=True)
        self.assertEqual(len(u._commands), 1)
        self.assertTrue('optimize' in u._commands[0])
        self.assertTrue(u._commands[0]['optimize']['waitFlush'])
        self.assertFalse(u._commands[0]['optimize']['waitSearcher'])

        u.clear_command()
        u.add_optimize(wait_searcher=True)
        self.assertEqual(len(u._commands), 1)
        self.assertTrue('optimize' in u._commands[0])
        self.assertFalse(u._commands[0]['optimize']['waitFlush'])
        self.assertTrue(u._commands[0]['optimize']['waitSearcher'])

    def _add_query_params(self, command, params):
        for param in params:
            command.add_param(param[0], param[1])

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



if __name__ == "__main__":
    # start the test server in a child process and pause while it starts up
    child_process = subprocess.Popen(['python', 'test_server.py'])
    time.sleep(5)
    try:
        unittest.main()
    finally:
        child_process.terminate()
