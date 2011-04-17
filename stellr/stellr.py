#   Copyright 2011 Michael Garski
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

import json
import urllib
from collections import Iterable

try:
    import httplib2
except ImportError:
    httplib2 = None

try:
    import tornado.httpclient as tornado
except ImportError:
    tornado = None

class StellrError(Exception):
    """Error that will be thrown from stellr."""
    def __init__(self, url, message):
        self.url = url
        self.message = message

class BaseCommand(object):
    def __init__(self, handler, content_type):
        self.handler = handler.lstrip('/').rstrip('/')
        self.content_type =  content_type
        self._commands = list()

    def clear_command(self):
        self._commands = list()

class UpdateCommand(BaseCommand):
    def __init__(self, commit_within=None,
                 commit=False, handler='/update/json'):
        BaseCommand.__init__(self, handler, 'application/json; charset=utf-8')
        self.commit_within = commit_within
        self.handler += '?wt=json'
        if commit:
            self.handler += '&commit=true'
        self.content_type = ''

    @property
    def data(self):
        return json.dumps(self._commands)

    def add_update(self, data):
        # check if data is an iterable or dictionary or object
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
        cmd = {'add': {'doc': doc}}
        if self.commit_within is not None:
            cmd['commitWithin'] = self.commit_within
        self._commands.append(cmd)

    def add_delete_by_id(self, data):
        # check if id is an iterable or value
        if isinstance(data, Iterable):
            for id in data:
                self._append_delete(id)
        else:
            self._append_delete(data)

    def _append_delete(self, id):
        self._commands.append({'delete': {'id': id}})

    def add_delete_by_query(self, data):
        # check if query is an iterable or value
        if isinstance(data, Iterable):
            for query in data:
                self._append_delete_query(query)
        else:
            self._append_delete_query(data)

    def _append_delete_query(self, query):
        self._commands.append({'delete': {'query': query}})

    def add_commit(self):
        self._commands.append({'commit': {}})

    def add_optimize(self, wait_flush=False, wait_searcher=False):
        self._commands.append({'optimize':
                                {'waitFlush':wait_flush,
                                 'waitSearcher':wait_searcher }})

class QueryCommand(BaseCommand):
    def __init__(self, handler='/solr/select'):
        BaseCommand.__init__(self, handler,
                             'application/x-www-form-urlencoded; \
                             charset=utf-8')

    def add_param(self, name, value):
        self._commands.append((name, value))

    @property
    def data(self):
        params = map(
                lambda x: '{0}={1}'.format(x[0], urllib.quote_plus(x[1])),
                     self._commands)
        return '&'.join(params)

class BaseConnection(object):
    def __init__(self, url, user=None, password=None, timeout=30000):
        self._url = url.rstrip('/') + '/'
        self._user = user
        self._password = password
        self._timeout = timeout

    def __str__(self):
        return 'host={0}, user={1}, password={2}'\
                .format(self._url, self._user, self._password)

class BlockingConnection(BaseConnection):
    def __init__(self, url, user=None, password=None):
        if not httplib2:
            raise StellrError('httplib2 is required for blocking \
                                   connections')
        BaseConnection.__init__(self, url, user, password)

    def execute(self, command):
        h = httplib2.Http(timeout=self._timeout)
        if self._user and self._password:
            h.add_credentials(self._user, self._password)
        try:
            url = self._url + command.handler
            body = command.data.encode('UTF-8')
            resp, content = h.request(url, 'POST', body=body,
                    headers={'content-type': command.content_type})
            if resp.status != 200:
                raise StellrError(url, 'HTTP {0} from Solr: {1}'
                                        .format(resp.status, content))
            return json.loads(content)
        except httplib2.HttpLib2Error as e:
            raise StellrError(url, str(e.__class__) + ' caught calling Solr.')
        except BaseException as e:
            if type(e) == StellrError:
                raise
            else:
                raise StellrError(url,
                        str(e.__class__) + ' caught when parsing json.')

class TornadoConnection(BaseConnection):
    def __init__(self, url, user=None, password=None, max_clients=10):
        if not tornado:
            raise StellrError('tornado.httpclient is required \
                                   for non-blocking connections')
        BaseConnection.__init__(self, url, user, password)
        self._max_clients = max_clients

    def execute_update(self, command, callback):
        pass

    def execute_query(self, command, callback):
        pass


class MockObj(object):
    pass

if __name__ == "__main__":
#    try:
#        test = BlockingConnection('test')
#        print(test)
#    except StellrError:
#        print("BlockingConnection failed")
#
#
#    try:
#        test = TornadoConnection("test", "u", "p")
#        print(test)
#    except StellrError:
#        print("TornadoConnection failed")
#
#    q = QueryCommand()
#    q.add_param('q', 'test')
#    q.add_param('qf', 'one,two')
#    print(q._commands)
#    print(q.data)
#
#    a = UpdateCommand(commit_within=1000)
#    a.add_update([{'d': 'val1', 'e': 2},{'d': 'val2', 'e':[1,2,'3']}])
#    print(a.data)
#
#    m = MockObj()
#    m.a = '123'
#    m.b = [1, 2, 3]
#    a.add_update(m)
#    print(a.data)
#
#    conn = BlockingConnection('http://localhost:8983')
#    query = QueryCommand(handler='/solr/topic/select')
#    query.add_param('q', 'a')
#    try:
#        print(conn.execute(query))
#    except StellrError as e:
#        print(e.url + '\n')
#        print(e.message)

    conn = BlockingConnection('http://localhost:8983',
                              user='admin', password='n0access')
    update = UpdateCommand(handler='/solr/topic/update/json',
                           commit=True)
    update.add_update({'id': 'mikey', 'topicname': 'mikey', 'topicuri':'/mikey', 'topictype':'mikey'})

    try:
        conn.execute(update)
    except StellrError as e:
        print e.url
        print(e.message)

    query = QueryCommand(handler='/solr/topic/select')
    query.add_param('q', 'id:mikey')
    try:
        print(conn.execute(query))
    except StellrError as e:
        print(e.url + '\n')
        print(e.message)





