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
    pass

class UpdateCommand(object):
    def __init__(self, commit_within=None, commit=False):
        self.commit_within = commit_within
        self.commit = commit
        self._commands = list()

    @property
    def data(self):
        return json.dumps(self._commands)

    def update(self, data):
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

    def delete_by_id(self, data):
        # check if id is an iterable or value
        if isinstance(data, Iterable):
            for id in data:
                self._append_delete(id)
        else:
            self._append_delete(data)

    def _append_delete(self, id):
        self._commands.append({'delete': {'id': id}})

    def delete_by_query(self, data):
        # check if query is an iterable or value
        if isinstance(data, Iterable):
            for query in data:
                self._append_delete_query(query)
        else:
            self._append_delete_query(data)

    def _append_delete_query(self, query):
        self._commands.append({'delete': {'query': query}})

    def commit(self):
        self._commands.append({'commit': {}})

    def optimize(self, wait_flush=False, wait_searcher=False):
        self._commands.append({'optimize':
                                {'waitFlush':wait_flush,
                                 'waitSearcher':wait_searcher }})

class QueryCommand(object):
    def __init__(self, handler='/solr/select'):
        self.handler = handler
        self._params = list()

    def add_param(self, name, value):
        self._params.append((name, value))

    @property
    def query(self):
        params = map(
                lambda x: '{0}={1}'.format(x[0], urllib.quote_plus(x[1])),
                     self._params)
        return '&'.join(params)

class BaseConnection(object):
    def __init__(self, url, user=None, password=None):
        self.url = url
        self.user = user
        self.password = password

    def execute_update(self, command):
        pass

    def execute_query(self, command):
        pass

    def __str__(self):
        return 'host={0}, user={1}, password={2}'\
            .format(self.url, self.user, self.password)

class BlockingConnection(BaseConnection):
    def __init__(self, url, user=None, password=None):
        if not httplib2:
            raise StellrError('httplib2 is required for blocking \
                                   connections')
        BaseConnection.__init__(self, url, user, password)

    def execute_update(self, command):
        pass

    def execute_query(self, command):
        pass

class TornadoConnection(BaseConnection):
    def __init__(self, url, user=None, password=None):
        if not tornado:
            raise StellrError('tornado.httpclient is required \
                                   for non-blocking connections')
        BaseConnection.__init__(self, url, user, password)

    def execute_update(self, command):
        pass

    def execute_query(self, command):
        pass


class MockObj(object):
    pass

if __name__ == "__main__":
    try:
        test = BlockingConnection('test')
        print(test)
    except StellrError:
        print("BlockingConnection failed")


    try:
        test = TornadoConnection("test", "u", "p")
        print(test)
    except StellrError:
        print("TornadoConnection failed")

    q = QueryCommand()
    q.add_param('q', 'test')
    q.add_param('qf', 'one,two')
    print(q._params)
    print(q.query)

    a = UpdateCommand(commit_within=1000)
    a.update([{'d': 'val1', 'e': 2},{'d': 'val2', 'e':[1,2,'3']}])
    print a.data

    m = MockObj()
    m.a = '123'
    m.b = [1, 2, 3]
    a.update(m)
    print a.data