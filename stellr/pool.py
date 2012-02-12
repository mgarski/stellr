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

import gevent.queue
from gevent_zeromq import zmq

class PoolManager(object):
    """
    The PoolManager is used to manage pools of ZeroMQ connections.

    size: the maximum size of each pool
    """

    def __init__(self, context, size=10):
        self.context = context
        self.size = size
        self.pools = {}

    def get_socket(self, address):
        """
        Get an open socket from a pool, creating a new pool and/or a new open
        socket as necessary.
        """
        pool = self.pools.get(address)
        if not pool:
            pool = gevent.queue.Queue(maxsize=self.size)
            self.pools[address] = pool
        if pool.empty():
            return self._create_socket(address)
        try:
            return pool.get_nowait()
        except gevent.queue.Empty:
            #TODO: his should not happen, log an error
            return self._create_socket(address)

    def replace_socket(self, address, socket):
        """
        Put a socket back into the pool unless it is full, then close and
        discard of it.
        """
        pool = self.pools.get(address)
        if not pool:
            #TODO: his should not happen, log an error
            self.destroy_socket(socket)
        elif pool.full():
            #TODO: this could happen, log a warning that the pool is full
            self.destroy_socket(socket)
        else:
            try:
                pool.put_nowait(socket)
            except gevent.queue.Full:
            #TODO: his should not happen, log an error
                self.destroy_socket(socket)

    def destroy_socket(self, socket):
        """
        Close a socket.
        """
        socket.setsockopt(zmq.LINGER, 0)
        socket.close()

    def _create_socket(self, address):
        socket = self.context.socket(zmq.REQ)
        socket.connect(address)
        return socket

class zmq_socket_pool(object):
    """
    The pool class provides access to the pol through a with statement
    that will automatically close a socket if the pool is full or an exception
    was raised during execution.
    """
    pool = None

    @classmethod
    def create(cls, context, size=10):
        """
        Create the connection pool.
        """
        zmq_socket_pool.pool = PoolManager(context, size)

    def __init__(self, address):
        self.address = address

    def __enter__(self):
        self.socket = zmq_socket_pool.pool.get_socket(self.address)
        return self.socket

    def __exit__(self, type, value, traceback):
        # was there an error? if so, ditch this socket
        if value:
            zmq_socket_pool.pool.destroy_socket(self.socket)
            return False
        else:
            zmq_socket_pool.pool.replace_socket(self.address, self.socket)
            return True