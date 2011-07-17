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
