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

# try simplejson first for the performance benefits
try:
    import simplejson as json
except ImportError:
    import json

# try importing the eventlet 'greened' libs
try:
    from eventlet.green import httplib
    from eventlet.green import urllib2
    import eventlet
except ImportError:
    # fall back to 'non-greened' libs
    import httplib
    import urllib2
    eventlet = None

# try importing Tornado libs
try:
    import tornado.httpclient as tornado_client
    import tornado.ioloop as tornado_loop
except ImportError:
    tornado_client = None
    tornado_loop = None

DEFAULT_TIMEOUT = 30
DEFAULT_MAX_TORNADO_CLIENTS = 10

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
        self.message = str(message)
        self.url = url
        self.timeout = timeout
        self.code = code

    def __str__(self):
        return self.message

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

class BaseConnection(object):
    """
    The BaseConnection is the base class for all connection objects and
    provides common instance fields and initialization. There are two
    parameters for initialization:

        address: the address, including the port (http://localhost:8983)
        timeout: the timeout value to use (in seconds, default=30)
    """
    def __init__(self, address, timeout=DEFAULT_TIMEOUT):
        self._address = address.rstrip('/')
        self._timeout = timeout

    def _build_err_msg(self, http_code):
        return 'HTTP %s: %s' % (http_code, httplib.responses[http_code])

class StandardConnection(BaseConnection):
    """
    A StandardConnection object will make the connection to the remote host
    and execute the command.There are two parameters for initialization:

        address: the address, including the port (e.g. http://localhost:8983)
        timeout: the timeout value to use (in seconds, default=30)
    """
    def __init__(self, address, timeout=DEFAULT_TIMEOUT):
        super(StandardConnection, self).__init__(address, timeout)

    def execute(self, command):
        """
        Execute the command, returning a dictionary with the results received
        from Solr.

        A StellrError will be raised if an error is encountered in
        communicating with Solr or in parsing the response.
        """
        url = self._address + command.handler
        request = urllib2.Request(url)
        request.add_header('content-type', command.content_type)
        data = command.data
        try:
            response = urllib2.urlopen(request, data, self._timeout)
            return json.load(response)
        except urllib2.HTTPError as e:
            print url
            raise StellrError(self._build_err_msg(e.code), url, code=e.code)
        except urllib2.URLError as e:
            if str(e.reason).find('timed out') >= 0:
                raise StellrError(self._build_err_msg(504), url, True, 504)
            else:
                raise StellrError(e.reason, url)
        except Exception as e:
            raise StellrError('Unexpected error encountered: %s.' % e, url)

class EventletConnection(StandardConnection):
    """
    An EventletConnection object will utilize the greened libraries that
    eventlet provides. There are two parameters for initialization:

        address: the address, including the port (e.g. http://localhost:8983)
        timeout: the timeout value to use (in seconds, default=30)
    """
    def __init__(self, address, timeout=DEFAULT_TIMEOUT):
        if not eventlet:
            raise StellrError("Unable to import eventlet.")
        super(EventletConnection, self).__init__(address, timeout)

    def execute(self, command):
        """
        Execute the command, returning a tuple with (command, response) with
        the response being a dictionary with the results reciveved from solr.
        This allows a client to execute a batch of commands and match up the
        response to the original command like so:

        ...
        pile = eventlet.GreenPile(pool)
        for command in commands:
            pile.spawn(connection.execute, command)
        ...

        The pile now contains an iterator over the tuples.

        A StellrError will be raised if an error is encountered in
        communicating with Solr or in parsing the response.
        """
        return command, super(EventletConnection, self).execute(command)

class TornadoConnection(BaseConnection):
    """
    The Tornado connection will execute the command using the non-blocking
    tornado.httpclient.AsyncHTTPClient.

    If the connection is initialized and the Tornado libraries cannot be
    imported or the IOLoop is not running, a StellrError will be raised.

    Initialization parameters:
        address: the address, including the port (e.g. http://localhost:8983)
        timeout: the timeout value to use (in seconds, default=30)
        max_clients: the maximum number of non-blocking calls to have at any
            time (default=10)
    """
    def __init__(self, address, timeout=DEFAULT_TIMEOUT,
                 max_clients=DEFAULT_MAX_TORNADO_CLIENTS):
        if not tornado_loop \
            and not tornado_loop.IOLoop.initialized() \
            and not tornado_loop.IOLoop.instance().running():
            raise StellrError(("Tornado's IOLoop is required to be "
                                "running to use the TornadoConnection"))
        super(TornadoConnection, self).__init__(address, timeout)
        self._max_clients = max_clients

    def execute(self, command, callback):
        """
        Execute the command using the non-blocking
        tornado.httpclient.AsyncHTTPClient, calling the value of the
        callback parameter passing an instance of StellrResponse as it's only
        parameter.

        Should an error occur in communicating with Solr or in parsing the
        response, the error property of the StellrResponse object passed to
        the callback method will contain a StellrError with the details.
        """
        self._callback = callback
        self._called_url = self._address + command.handler
        body = command.data
        request = tornado_client.HTTPRequest(self._called_url, method='POST',
                body=body, headers={'content-type': command.content_type},
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