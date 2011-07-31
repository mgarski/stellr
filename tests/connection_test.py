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

import mock
import unittest

import stellr.command as s_comm
import stellr.connection as s_conn

HDR_CONTENT_TYPE = 'Content-Type'
HDR_JSON = 'application/json'

TEST_HOST = 'http://localhost:8080'

CLAUSES = [('q', 'test query'), ('sort', 'name asc')]

DOCUMENTS = [
        ['a', 1, ['a1', '1a']],
        ['b', 2, ['b2', '2b']]]

FIELDS = ['field1', 'field2', 'listField']

class StellrConnectionTest(unittest.TestCase):
    """Test the stellr.connection module."""

    def test_standard_connection(self):
        """Test the StandardConnection."""
        conn = s_conn.StandardConnection(TEST_HOST)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        response = conn.execute(query)
        self.assertEquals(response['response']['q'], ['a'])

    def test_standard_connection_timeout(self):
        """Test the StandardConnection with a timeout."""
        conn = s_conn.StandardConnection(TEST_HOST, timeout=2)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        success = False
        try:
            conn.execute(query)
            success = True
        except s_conn.StellrError as e:
            self.assertTrue(e.timeout)

        self.assertFalse(success)

    def test_standard_connection_error(self):
        """Test the StandardConnection with an error."""
        conn = s_conn.StandardConnection(TEST_HOST)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        response = conn.execute(query)
        self.assertEquals(response['response']['q'], ['a'])

    def test_standard_connection_parsing_error(self):
        """Test the StandardConnection with a JSON parsing error."""
        conn = s_conn.StandardConnection(TEST_HOST)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        response = conn.execute(query)
        self.assertEquals(response['response']['q'], ['a'])

    def test_eventlet_connection(self):
        """Test the EventletConnection."""
        conn = s_conn.EventletConnection(TEST_HOST)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        response = conn.execute(query)
        self.assertEquals(response['response']['q'], ['a'])

    def test_tornado_connection(self):
        """Test the TornadoConnection."""
        conn = s_conn.TornadoConnection(TEST_HOST, max_clients=2)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')
        conn.execute(query, self._handle_response)
        self.assertTrue(False)

    def test_tornado_connection_timeout(self):
        """Test the TornadoConnection with a timeout."""
        conn = s_conn.TornadoConnection(TEST_HOST, timeout=2)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')
        query.add_param('s', '3')
        conn.execute(query, self._handle_timeout)
        self.assertTrue(False)

    def test_tornado_connection_error(self):
        """Test the TornadoConnection with an error."""
        conn = s_conn.TornadoConnection(TEST_HOST, timeout=2)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')
        query.add_param('s', '3')
        conn.execute(query, self._handle_timeout)
        self.assertTrue(False)

    def test_tornado_connection_parsing_error(self):
        """Test the TornadoConnection with a JSON parsing error."""
        conn = s_conn.TornadoConnection(TEST_HOST, timeout=2)
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')
        query.add_param('s', '3')
        conn.execute(query, self._handle_timeout)
        self.assertTrue(False)

    def _handle_response(self, response):
        self.assertFalse(response.error)
        self.assertEquals(response.body['response']['q'], ['a'])

    def _handle_timeout(self, response):
        self.assertTrue(response.error.timeout)