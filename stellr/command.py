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

from cStringIO import StringIO
import json
import urllib

CONTENT_FORM = 'application/x-www-form-urlencoded; charset=utf-8'
CONTENT_JSON = 'application/json; charset=utf-8'

class BaseCommand(object):
    """
    Base class for all commands. When overridden the BaseCommand needs to be
    initialized with two parameters:

        handler: the handler that will be called on the remote host
        content_type: the value to set the content-type header to when calling
            the handler on the remote host
    """
    def __init__(self, handler, content_type):
        self._handler = '/' + handler.lstrip('/').rstrip('/')
        self._query_string = [('wt', 'json')]
        self.content_type =  content_type
        self._commands = list()

    @property
    def data(self):
        """
        The data that is posted to the remote host, and must be implemented
        by all sub classes.
        """
        raise NotImplementedError

    @property
    def handler(self):
        """
        The handler and it's querystring, which will be appended to the
        address in the connection instance.
        """
        return '%s?%s' % (self._handler, urllib.urlencode(self._query_string))

    def clear_command(self):
        """
        Clear the command. This can be done after command execution to reuse
        the same instance.
        """
        self._commands = list()

class UpdateCommand(BaseCommand):
    """
    An UpdateCommand is used to submit updates to the remote host, and has
    the following initialization parameters:

        commit_within: integer value to use as the value to use for the number
            of milliseconds within the documents will be committed
            (default=None)
        commit: boolean value to inidcate whether a commit will be performed
            after the documents in the command are added (default=False)
        handler: the handler on the remote host that will be called
            (default='/solr/update/json')

    An UpdateCommand holds a list of commands that are performed in sequence
    on the remote host.
    """
    def __init__(self, commit_within=None,
                 commit=False, handler='/solr/update/json'):
        super(UpdateCommand, self).__init__(handler, CONTENT_JSON)
        self.commit_within = commit_within
        if commit:
            self._query_string.append(('commit', 'true'))
        elif commit_within is not None:
            self._query_string.append(('commitWithin', commit_within))

    @property
    def data(self):
        """
        The data posted to the remote host in the format specified at
        http://wiki.apache.org/solr/UpdateJSON. Duplicate names are valid JSON
        (http://www.ietf.org/rfc/rfc4627.txt section 2.2).
        """
        writer = StringIO()
        writer.write('{')
        for i in range(len(self._commands)):
            command = self._commands[i]
            writer.write('"%s": %s' % (command[0], json.dumps(command[1])))
            if i != len(self._commands) - 1:
                writer.write(',')
        writer.write('}')
        return writer.getvalue()

    def add_documents(self, data, boost=None):
        """
        Add a document or list of documents to the command that will be added
        to or updated in the index to be updated in the index. The value of
        the single parameter can be one of several things:

            1) dictionary: the keys are taken as field names with the
                corresponding values being the field values (lists are
                acceptable values for muti-valued fields)
            2) object: any valid obejct p the keys of the obejcts __dict__
                field are used as the field names and the corresponding
                values used as the field values (lists are acceptable values
                for multi-valued fields)
            3) a list of dictionaries or objects as in 1 & 2 above.

        Boosting a document is performed by specifying a float for the
        optional boost parameter to a float value. If an iterable of items is
        passed in then all of those items are added with the same boost value.

        Boosting a specific field on a document is done by setting the value
        of a field to the dictionary:
            { 'value': <field value>, 'boost': <boost value>}
        """
        if isinstance(data, dict):
            self._append_update(data, boost)
        elif isinstance(data, list):
            for doc in data:
                self._append_update(doc, boost)
        else:
            self._append_update(data, boost)

    def _append_update(self, doc, boost=None):
        # if an object, set the doc to its fields
        if hasattr(doc, '__dict__'):
            doc = doc.__dict__
        data = {'doc': doc}
        if boost:
            data['boost'] = boost
        self._commands.append(('add', data))

    def add_delete_by_id(self, data):
        """
        Add a delete to the command to delete an item by it's unique id in the
        index. The value of the parameter can be a single id or a list of ids.
        """
        if isinstance(data, list):
            for id in data:
                self._append_delete('id', id)
        else:
            self._append_delete('id', data)

    def add_delete_by_query(self, data):
        """
        Add a delete to the command to delete an item by a query. The value
        of the parameter can be a single query or a list of queries.
        """
        if isinstance(data, list):
            for query in data:
                self._append_delete('query', query)
        else:
            self._append_delete('query', data)

    def _append_delete(self, delete_type, data):
        self._commands.append(('delete', {delete_type: data}))

    def add_commit(self):
        """
        Add a commit to the command.
        """
        self._commands.append(('commit', {}))

    def add_optimize(self):
        """
        Add an optimize to the command. NOTE: a high timeout value may be
        desirable when optimizing an index. Should the call timeout the
        optimize operation will complete.
        """
        self._commands.append(('optimize', {}))

class SelectCommand(BaseCommand):
    """
    A SelectCommand can be used to issue any request against Solr by
    specifying the handler and adding named parameters to the instance. While
    the SelectCommand can be used for nearly all requests it may be helpful to
    subclass it for specific types of requests.
    """
    def __init__(self, handler='/solr/select'):
        super(SelectCommand, self).__init__(handler, CONTENT_FORM)

    def add_param(self, name, value):
        """
        Add a named parameter to the command. Names do not have to be unique
        and can be added multiple times.
        """
        value = unicode(value)
        self._commands.append((name, value.encode('utf-8')))

    @property
    def data(self):
        """
        The data that is posted to the remote host as a stirng of urlencoded
        string of key=value pairs delimited by &.
        """
        return urllib.urlencode(self._commands)