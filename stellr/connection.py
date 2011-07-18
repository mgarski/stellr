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
import httplib
import json
import platform

DEFAULT_TIMEOUT = 30
TIMEOUT_MSG = "HTTP 599: Operation timed out"

try:
    import tornado.httpclient as tornado_client
    import tornado.ioloop as tornado_loop
except ImportError:
    tornado_client = None
    tornado_loop = None

class StellrError(Exception):
    """
    Error that will be thrown from a Connection instance during the
    execution of a command. The error has the following fields avaialble:

        msg: a message with information about the error
        url: the url that was called
        timeout: a boolean indicating whether a timeout occurred
        code: the http error code received from the remote host, or if less
            than 0 the remote host was never called
    """
    def __init__(self, message, url=None, timeout=False, code=-1):
        super(Exception, self).__init__()
        self.msg = str(message)
        self.url = url
        self.timeout = timeout
        self.code = code

    def __str__(self):
        return self.msg

class StellrResponse(object):
    """
    The reponse that is returned from the execution of a blocking connection,
    or passed as the parameter to the callback on a non-blocking connection.
    This class has two properties:

        body: the body of the response from the remote host
        error: set to an instance of StellrError if an error is encountered
            during the execution of a non-blocking call
    """
    def __init__(self, body=None, error=None):
        self.body = body
        self.error = error

class Connection(object):
    """
    A Connection object will make the connection to the remote host.
    If the application is being run in the context of Tornado's IOLoop a
    command can be executed with the execute_non_blocking method, otherwise
    the execute method must be used to execute the command using httplib.

    Initialization parameters:
        host: the ip/host name of the remote host (REQUIRED)
        port: the port used to communicate to the host over (default=8983)
        user: the username to use for basic authentication (default=None)
        password: the password to use for basic authentication (default=None)
        timeout: the timeout value to use (in seconds, default=30)
        max_clients: the maximum number of non-blocking calls to have at any
            time (default=10)

    NOTE: the timeout will not be honored with a blocking connection on Mac OSX as
    sporadic socket errors will ensue.
    """
    def __init__(self, host, port=8983, user=None, password=None,
                 timeout=DEFAULT_TIMEOUT, max_clients=10):
        self._is_non_blocking = tornado_loop \
            and tornado_loop.IOLoop.initialized() \
            and tornado_loop.IOLoop.running()
        host = host.lstrip('http://')
        self._host = '%s:%i' % (host.rstrip('/'), port)
        self._user = user
        self._password = password
        self._timeout = self._set_timeout(timeout)
        self._max_clients = max_clients

    def is_non_blocking(self):
        """
        Return whether the connection instance will be non-blocking or not.
        """
        return self._is_non_blocking

    def __str__(self):
        return 'host=%s, user=%s, password=%s, max clients=%i' % \
               (self._host, self._user, self._password, self._max_clients)

    def _build_err_msg(self, http_code):
        return 'HTTP %s: %s' % (http_code, httplib.responses[http_code])

    def _set_timeout(self, timeout):
        # no timeout for mac with a blocking connection
        if not self.is_non_blocking() and platform.mac_ver()[0]:
            return None
        else:
            return timeout

    def execute_blocking(self, command):
        """
        Executes the command using a blocking connection (httplib) and
        returns an instance of SolrResponse.
        """
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

    def execute_non_blocking(self, command, callback):
        """
        Execute the command using a non-blocking connection, calling the
        value of the callback parameter passing an instance of StellrResponse
        as it's only parameter.

        If this method is called without being in the context of a currently
        running Tornado IOLoop, a StellrError will be raised.
        """
        if not self.is_non_blocking():
            raise StellrError('No IOLoop context found '
                              'to make non-blocking call.')
        self._callback = callback
        self._called_url = 'http://' + self._host + command.handler
        body = command.data
        request = tornado_client.HTTPRequest(self._called_url, method='POST',
                body=body, headers={'content-type': command.content_type},
                auth_username=self._user, auth_password=self._password,
                request_timeout=self._timeout)
        h = tornado_client.AsyncHTTPClient(max_clients=self._max_clients)
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