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
    import tornado.httpclient as tornadolib
except ImportError:
    tornadolib = None

class StellrError(Exception):
    """Error that will be thrown from stellr."""
    def __init__(self, message, url=None, timeout=False, code=-1):
        self.msg = str(message)
        self.url = url
        self.timeout = timeout
        self.code = code

    def __str__(self):
        return self.msg

class StellrResponse(object):
    def __init__(self, body=None, error=None):
        self.body = body
        self.error = error

class BaseCommand(object):
    def __init__(self, handler, content_type):
        self.handler = '/' + handler.lstrip('/').rstrip('/')
        self.handler += '?wt=json'
        self.content_type =  content_type
        self._commands = list()

    def clear_command(self):
        self._commands = list()

class UpdateCommand(BaseCommand):
    def __init__(self, commit_within=None,
                 commit=False, handler='/solr/update/json'):
        BaseCommand.__init__(self, handler, 'application/json; charset=utf-8')
        self.commit_within = commit_within
        if commit:
            self.handler += '&commit=true'
        elif commit_within is not None:
            self.handler += '&commitWithin=' + str(commit_within)

    @property
    def data(self):
        ret_val = StringIO()
        ret_val.write('{')
        for i in range(len(self._commands)):
            command = self._commands[i]
            ret_val.write('"%s": %s' % (command[0], json.dumps(command[1])))
            if i != len(self._commands) - 1:
                ret_val.write(',')
        ret_val.write('}')
        return ret_val.getvalue()

    def add_update(self, data):
        if type(data) is dict:
            self._append_update(data)
        elif isinstance(data, Iterable):
            for doc in data:
                self._append_update(doc)
        else:
            self._append_update(data)

    def _append_update(self, doc):
        if hasattr(doc, '__dict__'):
            doc = doc.__dict__
        self._commands.append(('add', {'doc': doc}))

    def add_delete_by_id(self, data):
        if isinstance(data, list):
            for id in data:
                self._append_delete('id', id)
        else:
            self._append_delete('id', data)

    def add_delete_by_query(self, data):
        if isinstance(data, list):
            for query in data:
                self._append_delete('query', query)
        else:
            self._append_delete('query', data)

    def _append_delete(self, delete_type, data):
        self._commands.append(('delete', {delete_type: data}))

    def add_commit(self):
        self._commands.append(('commit', {}))

    def add_optimize(self):
        self._commands.append(('optimize', {}))

class QueryCommand(BaseCommand):
    def __init__(self, handler='/solr/select'):
        BaseCommand.__init__(self, handler,
                             'application/x-www-form-urlencoded; \
                             charset=utf-8')

    def add_param(self, name, value):
        value = unicode(value)
        self._commands.append((name, value.encode('utf-8')))

    @property
    def data(self):
        return urllib.urlencode(self._commands)

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