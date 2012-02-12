#   Copyright 2011-2012 Michael Garski (mgarski@mac.com)
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

from mock import patch, Mock, MagicMock
import unittest
import gevent.queue
from gevent_zeromq import zmq

import stellr

ADDRESS = 'tcp://1.2.3.4:69'

class PoolTest(unittest.TestCase):
    """Perform tests on the pool module."""

    def pool_manager_creation_test(self):
        """Test the creation of the PoolManager."""
        context = Mock()
        p = stellr.pool.PoolManager(context, 42)
        self.assertEquals(p.context, context)
        self.assertEquals(p.size, 42)
        self.assertEquals(p.pools, {})

    @patch('stellr.pool.PoolManager')
    def pool_creation_test(self, pool_mgr):
        """Test the creation of the pool through the context."""
        mgr = Mock()
        pool_mgr.return_value = mgr
        context = Mock()

        stellr.pool.zmq_socket_pool.create(context, 69)

        self.assertEquals(stellr.pool.zmq_socket_pool.pool, mgr)
        pool_mgr.assert_called_once_with(context, 69)

    def create_socket_test(self):
        """Test the _create_socket method."""
        context = Mock()
        p = stellr.pool.PoolManager(context)
        socket = Mock()
        context.socket.return_value = socket

        s = p._create_socket(ADDRESS)
        self.assertEqual(s, socket)
        context.socket.assert_called_once_with(zmq.REQ)
        socket.connect.assert_called_once_with(ADDRESS)

    def destroy_socket_test(self):
        """Test the destroy_socket method."""
        p = stellr.pool.PoolManager(Mock())
        socket = Mock()
        p.destroy_socket(socket)
        socket.setsockopt.assert_called_once_with(zmq.LINGER, 0)

    @patch('gevent.queue.Queue')
    def get_socket_empty_queue_test(self, q):
        """Test getting a socket with an empty pool."""
        queue = Mock()
        queue.empty.return_value = True
        q.return_value = queue
        context = Mock()
        p = stellr.pool.PoolManager(context)
        socket = Mock()
        socket.return_value = socket
        p._create_socket = socket

        s = p.get_socket(ADDRESS)
        self.assertEqual(s, socket)
        self.assertEqual(1, queue.empty.call_count)
        self.assertEqual(1, len(p.pools))

    @patch('gevent.queue.Queue')
    def get_socket_empty_queue_empty_error_test(self, q):
        """Test getting a socket with pool that throws an empty error."""
        queue = Mock()
        queue.empty.return_value = False
        queue.get_nowait.side_effect = gevent.queue.Empty
        q.return_value = queue
        context = Mock()
        p = stellr.pool.PoolManager(context)
        socket = Mock()
        socket.return_value = socket
        p._create_socket = socket

        s = p.get_socket(ADDRESS)
        self.assertEqual(s, socket)
        self.assertEqual(1, queue.empty.call_count)
        self.assertEqual(1, len(p.pools))

    @patch('gevent.queue.Queue')
    def get_socket_non_empty_queue_test(self, q):
        """Test getting a socket with a non-empty pool."""
        socket = Mock()
        queue = Mock()
        queue.empty.return_value = False
        queue.get_nowait.return_value = socket
        q.return_value = queue
        context = Mock()
        p = stellr.pool.PoolManager(context)
        p.pools[ADDRESS] = queue

        s = p.get_socket(ADDRESS)
        self.assertEqual(s, socket)
        self.assertEqual(1, queue.empty.call_count)
        self.assertEqual(1, len(p.pools))

    def replace_socket_success_test(self):
        """Test successfully replacing a socket."""
        p = stellr.pool.PoolManager(Mock())
        q = Mock()
        q.full.return_value = False
        p.pools[ADDRESS] = q
        s = Mock()

        p.replace_socket(ADDRESS, s)
        self.assertEqual(1, q.full.call_count)
        q.put_nowait.assert_called_once_with(s)

    def replace_socket_no_queue_test(self):
        """Test replacing a socket with no pool to put it in."""
        p = stellr.pool.PoolManager(Mock())
        destroy = Mock()
        p.destroy_socket = destroy
        socket = Mock()
        p.replace_socket(ADDRESS, socket)
        destroy.assert_called_once_with(socket)

    def replace_socket_full_queue_test(self):
        """Test replacing a socket into a full pool."""
        q = Mock()
        q.full.return_value = True
        socket = Mock()
        p = stellr.pool.PoolManager(Mock())
        p.pools[ADDRESS] = q
        destroy = Mock()
        p.destroy_socket = destroy
        p.replace_socket(ADDRESS, socket)
        destroy.assert_called_once_with(socket)

    def replace_socket_full_queue_full_error_test(self):
        """Test replacing a socket into a full pool."""
        q = Mock()
        q.full.return_value = False
        q.put_nowait.side_effect = gevent.queue.Full
        socket = Mock()
        p = stellr.pool.PoolManager(Mock())
        p.pools[ADDRESS] = q
        destroy = Mock()
        p.destroy_socket = destroy
        p.replace_socket(ADDRESS, socket)
        destroy.assert_called_once_with(socket)

    def enter_context_test(self):
        """Test entering the context."""
        z = stellr.pool.zmq_socket_pool(ADDRESS)
        socket = Mock()
        pool = Mock()
        pool.get_socket.return_value = socket
        stellr.pool.zmq_socket_pool.pool = pool
        with z as f:
            self.assertEqual(f, socket)
        pool.get_socket.assert_called_once_with(ADDRESS)
        pool.replace_socket.assert_called_once_with(ADDRESS, socket)

    def enter_context_error_test(self):
        """Test entering the context."""
        z = stellr.pool.zmq_socket_pool(ADDRESS)
        socket = Mock()
        pool = Mock()
        pool.get_socket.return_value = socket
        stellr.pool.zmq_socket_pool.pool = pool
        try:
            with z as f:
                self.assertEqual(f, socket)
                raise Exception()
        except Exception:
            pass
        pool.get_socket.assert_called_once_with(ADDRESS)
        pool.destroy_socket.assert_called_once_with(socket)