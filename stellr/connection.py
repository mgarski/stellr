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

import base64
from collections import Iterable
from cStringIO import StringIO
import httplib
import json
import platform
import urllib

DEFAULT_TIMEOUT = 30
TIMEOUT_MSG = "HTTP 599: Operation timed out"

try:
    import tornado.httpclient as tornado_client
    import tornado.ioloop as tornado_loop
except ImportError:
    tornado_client = None
    tornado_loop = None

class StellrError(Exception):
    """Error that will be thrown from stellr."""
    def __init__(self, message, url=None, timeout=False, code=-1):
        self.msg = str(message)
        self.url = url
        self.timeout = timeout
        self.code = code

    def __str__(self):
        return self.msg

class Connection(object):
    def __init__(self, host, port=8983, user=None, password=None,
                 timeout=DEFAULT_TIMEOUT, max_clients=10):
        """Create a instance of an object that will make the connection
        to the remote host. If the application is being run in the context
        of Tornado's IOLoop the connection will be non-blocking, otherwise
        the connection made will be a blocking connection using httplib. The
        timeout will not be honored with a blocking connection on Mac OSX as
        sporadic socket errors will ensue.
        """                                                                                                     ""
        self._host = '%s:%i' % (host.rstrip('/'), port)
        self._user = user
        self._password = password
        self._timeout = self._set_timeout(timeout)
        self._max_clients = max_clients

    def is_non_blocking(self):
        """Return whether the connection instance will be
        non-blocking or not."""
        return tornado_loop \
            and tornado_loop.IOLoop.initialized() \
            and tornado_loop.IOLoop.running()

    def __str__(self):
        return 'host=%s, user=%s, password=%s, max clients=%i' % \
               (self._host, self._user, self._password, self._max_clients)

    def _build_err_msg(self, http_code):
        return 'HTTP %s: %s' % (http_code, httplib.responses[http_code])

    def _set_timeout(self, timeout):
        # no timeout for mac with a blocking connection
        # or sporadic socket errors will ensue
        if not self.is_non_blocking() and platform.mac_ver()[0]:
            return None
        else:
            return timeout



class BaseConnection(object):
    def __init__(self, host, user=None, password=None,
                 timeout=DEFAULT_TIMEOUT):
        self._host = host.rstrip('/')
        self._user = user
        self._password = password
        self._timeout = timeout


    def __str__(self):
        return 'host=%s, user=%s, password=%s' % \
               (self._host, self._user, self._password)

    def _build_err_msg(self, http_code):
        return 'HTTP %s: %s' % (http_code, httplib.responses[http_code])

class BlockingConnection(BaseConnection):
    def __init__(self, host, user=None, password=None,
                 timeout=DEFAULT_TIMEOUT):
        host = host.lstrip('http://')
        BaseConnection.__init__(self, host, user, password, timeout)
        # no timeout for mac or sporadic socket errors will ensue
        if platform.mac_ver()[0]:
            self._timeout = None
        else:
            self._timeout = timeout

    def execute(self, command):
        conn = httplib.HTTPConnection(self._host, timeout=self._timeout)
        data = command.data
        headers = self._assemble_headers(command.content_type)
        try:
            conn.request('POST', command.handler, data, headers)
            response = conn.getresponse()
            if response.status == 200:
                return json.loads(response.read())
            else:
                raise StellrError(self._build_err_msg(response.status),
                                  url=self._host + command.handler,
                                  code=response.status)
        except StellrError:
            raise
        except Exception as e:
            raise StellrError(e, self._host + command.handler, code=500)
        finally:
            conn.close()

    def _assemble_headers(self, content_type):
        headers = {'content-type': content_type}
        if self._user and self._password:
            auth = self._user + ':' + self._password
            auth = 'Basic ' + base64.b64encode(auth)
            headers['Authorization'] = auth
        return headers


class TornadoConnection(BaseConnection):
    def __init__(self, url, user=None, password=None,
                 max_clients=10, timeout=DEFAULT_TIMEOUT):
        if not tornadolib:
            raise StellrError(None, 'tornado.httpclient is required \
                                   for non-blocking connections')
        BaseConnection.__init__(self, url, user, password, timeout)
        self._max_clients = max_clients

    def execute(self, command, callback):
        self._callback = callback
        self._called_url = self._host + command.handler
        body = command.data
        request = tornadolib.HTTPRequest(self._called_url, method='POST',
                body=body, headers={'content-type': command.content_type},
                auth_username=self._user, auth_password=self._password,
                request_timeout=self._timeout)
        h = tornadolib.AsyncHTTPClient(max_clients=self._max_clients)
        h.fetch(request, self._receive_solr)

    def _receive_solr(self, response):
        sr = StellrResponse()
        if response.error:
            error = response.error
            timeout = False
            try:
                timeout = error.errno == 28 and error.code == 599
            except AttributeError:
                timeout = error.code == 599
            error.code = error.code if error.code != 599 else 504
            sr.body = response.body
            sr.error = StellrError(error, url=self._called_url,
                                   code=error.code, timeout=timeout)
        else:
            try:
                sr.body = json.loads(response.body)
            except Exception as e:
                sr.body = response.body
                sr.error = StellrError(
                    'Response body could not be parsed: %s' % e,
                    url=self._called_url)
        self._callback(sr)