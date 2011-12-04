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

import datetime
from nose.tools import raises
import simplejson as json
import unittest

import stellr.command as command

HDR_CONTENT_TYPE = 'Content-Type'
HDR_JSON = 'application/json'

TEST_HOST = 'http://localhost:8080'

CLAUSES = [('q', 'test query'), ('sort', 'name asc')]

DOCUMENTS = [
        ['a', 1, ['a1', '1a']],
        ['b', 2, ['b2', '2b']]]

FIELDS = ['field1', 'field2', 'listField']

class SimpleObject(object):
    """Simple object used in testing addition of documents."""
    def __init__(self, field1, field2, listField):
        self.field1 = field1
        self.field2 = field2
        self.listField = listField

class StellrCommandTest(unittest.TestCase):
    """Perform tests on the stellr.command module."""

    @raises(NotImplementedError)
    def test_bad_child(self):
        """Test an incorrectly subclassed command throws an error"""
        c = command.BaseCommand('spam', 'eggs')
        return c.data

    def test_select_command(self):
        """Test the SelectCommand."""
        q = command.SelectCommand(handler='/solr/test/search/')
        self.assertEqual(q.handler, '/solr/test/search?wt=json')

        self._add_query_params(q, CLAUSES)
        self.assertEqual(q._commands, CLAUSES)

        self.assertEqual(q.data, 'q=test+query&sort=name+asc')

        q.clear_command()
        self.assertEqual(len(q._commands), 0)

    def test_update(self):
        """Test the UpdateCommand with document updates."""
        u = command.UpdateCommand(commit_within=60000)
        self.assertEqual(u.handler, ('/solr/update/json?'
                                     'wt=json&commitWithin=60000'))

        a = SimpleObject(DOCUMENTS[0][0], DOCUMENTS[0][1], DOCUMENTS[0][2])
        u.add_documents(a)

        b = dict()
        for i, field in enumerate(FIELDS):
            b[field] = DOCUMENTS[1][i]
        u.add_documents(b)

        self.assertEqual(len(u._commands), 2)
        for i, comm in enumerate(u._commands):
            self.assertEqual(comm[0], 'add')
            self.assertTrue('doc' in comm[1])
            for field, value in comm[1]['doc'].iteritems():
                field_ord = FIELDS.index(field)
                self.assertEqual(DOCUMENTS[i][field_ord], value)

    def test_update_list(self):
        """Test the UpdateCommand with a list of updates."""
        u = command.UpdateCommand()
        docs = [{'a': 1}, {'b': 2}]
        u.add_documents(docs)
        self.assertEqual(2, len(u._commands))
        self.assertEqual(u.data,
                         ('{"add": {"doc": {"a": 1}}'
                          ',"add": {"doc": {"b": 2}}}'))

    def test_update_with_document_boost(self):
        """Test the UpdateCommand with a document boost."""
        u = command.UpdateCommand()
        u.add_documents({'a': 1}, boost=2.0)
        self.assertEqual(u.data, '{"add": {"doc": {"a": 1}, "boost": 2.0}}')

    def test_update_with_field_boost(self):
        """Test the UpdateCommand with a document containing a field boost."""
        u = command.UpdateCommand()
        u.add_documents({'a': { 'value': 'f', 'boost': 2.0}})
        self.assertEqual(u.data, ('{"add": {"doc": {"a": '
                                  '{"boost": 2.0, "value": "f"}}}}'))

    def test_delete(self):
        """Test the UpdateCommand with deletes."""
        u = command.UpdateCommand()
        u.add_delete_by_id(0)
        u.add_delete_by_id([1, 2])
        self.assertTrue(len(u._commands), 3)
        for i, delete in enumerate(u._commands):
            self.assertEquals(delete, ('delete', {'id': str(i)}))
        self.assertEqual(u.data,
                         ('{"delete": {"id": "0"},"delete": {"id": "1"}'
                          ',"delete": {"id": "2"}}'))

        u.clear_command()
        u.add_delete_by_id(0)
        self.assertTrue(len(u._commands), 1)
        self.assertEqual(u.data, '{"delete": {"id": "0"}}')

        u.clear_command()
        self.assertEqual(0, len(u._commands))
        u.add_delete_by_query('field1:value0')
        u.add_delete_by_query(['field1:value1', 'field1:value2'])
        self.assertTrue(len(u._commands), 3)
        for i, delete in enumerate(u._commands):
            self.assertEquals(delete, ('delete',
                                       {'query': 'field1:value' + str(i)}))
        self.assertEqual(u.data,
                         ('{"delete": {"query": "field1:value0"}'
                          ',"delete": {"query": "field1:value1"}'
                          ',"delete": {"query": "field1:value2"}}'))

    def test_commit(self):
        """Test adding or specifying a commit on a command."""
        u = command.UpdateCommand(commit=True)
        self.assertEqual(u.handler, '/solr/update/json?wt=json&commit=true')

        u.add_commit()
        self.assertEqual(len(u._commands), 1)
        self.assertEqual(('commit', {}), u._commands[0])
        self.assertEqual(u.data, '{"commit": {}}')

    def test_optimize(self):
        """Test adding an optimize operation to a command."""
        u = command.UpdateCommand()

        u.add_optimize()
        self.assertEqual(len(u._commands), 1)
        self.assertTrue('optimize' in u._commands[0])
        self.assertEqual(u.data, '{"optimize": {}}')

    def _add_query_params(self, command, params):
        for param in params:
            command.add_param(param[0], param[1])

class StellrJSONEncoderTest(unittest.TestCase):
    """Test the JSON encoder."""

    def test_datetime_encoding(self):
        """Test the JSON encoder for datetime instances."""
        data = {
            'date': datetime.datetime(1970, 2, 3, 11, 20, 42),
            'int': 4,
            'str': 'string'
        }
        s = json.dumps(data, cls=command.StellrJSONEncoder)
        self.assertEqual(
            s, '{"date": "1970-02-03T11:20:42Z", "int": 4, "str": "string"}')