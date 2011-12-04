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

from mock import patch, Mock
from eventlet.green import urllib2
from eventlet.green import zmq
from nose.tools import raises
import simplejson as json
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

    def setUp(self):
        """Set up the patched libs."""
        self.req_patch = patch('eventlet.green.urllib2.Request')
        self.mock_req = self.req_patch.start()
        self.mock_req_ret = Mock()
        self.mock_req.return_value = self.mock_req_ret

        self.urlopen_patch = patch('eventlet.green.urllib2.urlopen')
        self.mock_urlopen = self.urlopen_patch.start()
        response = Mock()
        response.read.return_value = json.dumps({'response': {'q': 'a'}})
        self.mock_urlopen.return_value = response
        # reset the context to None
        s_conn.ZeroMQConnection.context = None


    def tearDown(self):
        """Tear down the patches."""
        self.urlopen_patch.stop()
        self.req_patch.stop()

    def test_standard_connection(self):
        """Test the StandardConnection."""
        conn = s_conn.StandardConnection(TEST_HOST)
        self.assertEquals(TEST_HOST, conn._address)
        self.assertEquals(s_conn.DEFAULT_TIMEOUT, conn._timeout)

        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        response = conn.execute(query)
        self.assertEquals(response['response']['q'], 'a')
        self.mock_req.assert_called_once_with(TEST_HOST + '/query?wt=json')
        self.mock_req.add_header.called_once_with('content-type',
                                                  'application/json')
        self.mock_urlopen.assert_called_once_with(
            self.mock_req_ret, 'q=a', 30)

    def test_standard_connection_timeout(self):
        """Test the StandardConnection with a timeout."""
        conn = s_conn.StandardConnection(TEST_HOST, timeout=2)
        self.assertEquals(TEST_HOST, conn._address)
        self.assertEquals(2, conn._timeout)

        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        self.mock_urlopen.side_effect = urllib2.URLError(
            'the call timed out to the server')
        try:
            conn.execute(query)
        except s_conn.StellrError as e:
            self.assertTrue(e.timeout)
        else:
            self.fail(msg='No exception raised in timeout.')

    def test_standard_connection_error(self):
        """Test the StandardConnection with an error."""
        conn = s_conn.StandardConnection(TEST_HOST)

        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        # http-related error
        self.mock_urlopen.side_effect = urllib2.HTTPError(
            'url', 404, 'error', {}, open(__file__))
        try:
            conn.execute(query)
        except s_conn.StellrError as e:
            self.assertFalse(e.timeout)
        else:
            self.fail(msg='No exception raised.')

        # non-timeout URLError
        self.mock_urlopen.side_effect = urllib2.URLError(
            'this is a non-timeout error')
        try:
            conn.execute(query)
        except s_conn.StellrError as e:
            self.assertFalse(e.timeout)
        else:
            self.fail(msg='No exception raised.')

    @patch('simplejson.loads')
    def test_standard_connection_parsing_error(self, mock_json):
        """Test the StandardConnection with a JSON parsing error."""
        conn = s_conn.StandardConnection(TEST_HOST)

        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        mock_json.side_effect = Exception('error')
        try:
            response = conn.execute(query)
        except s_conn.StellrError as e:
            self.assertEquals(e.message, 'Unexpected error encountered: error.')
        else:
            self.fail(msg='No exception raised.')

    def test_eventlet_connection(self):
        """Test the EventletConnection."""
        conn = s_conn.EventletConnection(TEST_HOST)
        self.assertEquals(TEST_HOST, conn._address)
        self.assertEquals(s_conn.DEFAULT_TIMEOUT, conn._timeout)

        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        response = conn.execute(query)
        self.assertEquals(response[0], query)
        self.assertEquals(response[1]['response']['q'], 'a')
        self.mock_req.assert_called_once_with(TEST_HOST + '/query?wt=json')
        self.mock_req.add_header.called_once_with('content-type',
                                                  'application/json')
        self.mock_urlopen.assert_called_once_with(
            self.mock_req_ret, 'q=a', 30)

    @patch('tornado.httpclient')
    def test_tornado_connection(self, t_mock):
        """Test the TornadoConnection."""
        conn = s_conn.TornadoConnection(TEST_HOST, max_clients=2)
        self.assertEquals(TEST_HOST, conn._address)
        self.assertEquals(s_conn.DEFAULT_TIMEOUT, conn._timeout)
        self.assertEquals(2, conn._max_clients)

        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')

        #TODO: mock tornado
        conn.execute(query, self._handle_response)

    def test_tornado_connection_timeout(self):
        """Test the TornadoConnection with a timeout."""
        conn = s_conn.TornadoConnection(TEST_HOST, timeout=2)
        self.assertEquals(TEST_HOST, conn._address)
        self.assertEquals(2, conn._timeout)
        self.assertEquals(s_conn.DEFAULT_MAX_TORNADO_CLIENTS,
                          conn._max_clients)

        #TODO: mock command object
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')
        query.add_param('s', '3')

        #TODO: mock tornado
        conn.execute(query, self._handle_timeout)

    def test_tornado_connection_error(self):
        """Test the TornadoConnection with an error."""
        conn = s_conn.TornadoConnection(TEST_HOST, timeout=2)

        #TODO: mock command object
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')
        query.add_param('s', '3')
        conn.execute(query, self._handle_timeout)

    def test_tornado_connection_parsing_error(self):
        """Test the TornadoConnection with a JSON parsing error."""
        conn = s_conn.TornadoConnection(TEST_HOST, timeout=2)

        #TODO: mock command object
        query = s_comm.SelectCommand(handler='/query')
        query.add_param('q', 'a')
        query.add_param('s', '3')

        #TODO: mock tornado
        conn.execute(query, self._handle_timeout)

    @raises(s_conn.StellrError)
    def test_zeromq_context_not_set(self):
        """Test to verify the context must be set."""
        x = s_conn.ZeroMQConnection('address', 10)

    def test_zeromq_init(self):
        """Test to verify the instance is properly created."""
        s_conn.ZeroMQConnection.set_context(Mock())
        x = s_conn.ZeroMQConnection('address', 10)
        self.assertEqual(10000, x._timeout)

    @patch('eventlet.green.zmq.Poller')
    def test_zeromq_connection_success(self, zp):
        """Test the zero mq connection with a success."""
        context = Mock()
        socket = Mock()
        socket.recv.return_value = '{"key": "value"}'
        context.socket.return_value = socket
        s_conn.ZeroMQConnection.set_context(context)

        p = Mock()
        p.poll.return_value = [(socket, zmq.POLLIN)]
        zp.return_value = p

        conn = s_conn.ZeroMQConnection('host:port')
        comm = s_comm.SelectCommand(handler='/handler')
        comm.add_param('q', 'query')
        result = conn.execute(comm)
        p.register.assert_called_once_with(socket, zmq.POLLIN)
        socket.connect.assert_called_once_with('host:port')
        socket.send.assert_called_once_with('/handler?wt=json q=query')
        socket.close.assert_called_once()
        p.poll.assert_called_once_with(30000)
        self.assertEqual(result, {'key': 'value'})

    @patch('eventlet.green.zmq.Poller')
    def test_zeromq_connection_timeout(self, zp):
        """Test the zero mq connection with a success."""
        context = Mock()
        socket = Mock()
        socket.recv.return_value = '{"key": "value"}'
        context.socket.return_value = socket
        s_conn.ZeroMQConnection.set_context(context)

        p = Mock()
        p.poll.return_value = [(socket, zmq.POLLIN - 1)]
        zp.return_value = p

        conn = s_conn.ZeroMQConnection('host:port')
        comm = s_comm.SelectCommand(handler='/handler')
        comm.add_param('q', 'query')
        result = None
        try:
            result = conn.execute(comm)
        except s_conn.StellrError as e:
            self.assertEqual(
                e.message, 'Unexpected error: HTTP 504: Gateway Timeout')
        p.register.assert_called_once_with(socket, zmq.POLLIN)
        socket.connect.assert_called_once_with('host:port')
        socket.send.assert_called_once_with('/handler?wt=json q=query')
        socket.close.assert_called_once()
        p.poll.assert_called_once_with(30000)
        self.assertEqual(result, None)

    def _handle_response(self, response):
        self.assertFalse(response.error)
        self.assertEquals(response.body['response']['q'], ['a'])

    def _handle_timeout(self, response):
        self.assertTrue(response.error.timeout)