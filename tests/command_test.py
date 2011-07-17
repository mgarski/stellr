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
        self.assertEqual(u.handler, ('/solr/update/json?'
                                     'wt=json&commitWithin=60000'))

        a = MockObject(DOCUMENTS[0][0], DOCUMENTS[0][1], DOCUMENTS[0][2])
        u.add_update(a)

        b = dict()
        for i, field in enumerate(FIELDS):
            b[field] = DOCUMENTS[1][i]
        u.add_update(b)

        self.assertEqual(len(u._commands), 2)
        for i, command in enumerate(u._commands):
            self.assertEqual(command[0], 'add')
            self.assertTrue('doc' in command[1])
            for field, value in command[1]['doc'].iteritems():
                field_ord = FIELDS.index(field)
                self.assertEqual(DOCUMENTS[i][field_ord], value)

    def test_delete(self):
        u = stellr.UpdateCommand()
        u.add_delete_by_id(0)
        u.add_delete_by_id([1, 2])
        self.assertTrue(len(u._commands), 3)
        for i, delete in enumerate(u._commands):
            self.assertEquals(delete, ('delete', {'id': i}))

        u.clear_command()
        u.add_delete_by_query('field1:value0')
        u.add_delete_by_query(['field1:value1', 'field1:value2'])
        self.assertTrue(len(u._commands), 3)
        for i, delete in enumerate(u._commands):
            self.assertEquals(delete, ('delete',
                                       {'query': 'field1:value' + str(i)}))

    def test_commit(self):
        u = stellr.UpdateCommand(commit=True)
        self.assertEqual(u.handler, '/solr/update/json?wt=json&commit=true')

        u.add_commit()
        self.assertEqual(len(u._commands), 1)
        self.assertEqual(('commit', {}), u._commands[0])

    def test_optimize(self):
        u = stellr.UpdateCommand()

        u.add_optimize()
        self.assertEqual(len(u._commands), 1)
        self.assertTrue('optimize' in u._commands[0])

    def _add_query_params(self, command, params):
        for param in params:
            command.add_param(param[0], param[1])