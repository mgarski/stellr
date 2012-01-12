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
import datetime
import eventlet
from eventlet.green import urllib
urllib3 = eventlet.import_patched('urllib3')

# try simplejson first
try:
    import simplejson as json
except ImportError:
    import json

CONTENT_FORM = 'application/x-www-form-urlencoded; charset=utf-8'
CONTENT_JSON = 'application/json; charset=utf-8'
DEFAULT_TIMEOUT = 15

# the pool of connections
pool = urllib3.PoolManager(maxsize=25)

class StellrError(Exception):
    """
    Error that will be thrown from a Connection instance during the
    execution of a command. The error has the following fields available:

        msg: a message with information about the error
        url: the url that was called
        timeout: a boolean indicating whether a timeout occurred
        code: the http error code received from the remote host, or if less
            than 0 the remote host was never called
    """
    def __init__(self, message, url=None, body=None, response=None,
                 timeout=False, status=-1):
        super(Exception, self).__init__()
        self.message = str(message)
        self.url = url
        self.body = body
        self.response = response
        self.timeout = timeout
        self.status = status

    def __str__(self):
        return self.message

class BaseCommand(object):
    """
    Base class for all commands. When overridden the BaseCommand needs to be
    initialized with two parameters:

        host: the Solr host, such as http://localhost:8983/
        handler: the handler that will be called on the remote host
        content_type: the value to set the content-type header to when calling
            the handler on the remote host
    """

    def __init__(self, host, handler, timeout, name, content_type):
        global pool
        self.pool = pool
        self.url = '%s/%s?wt=json' % \
                   (host.rstrip('/'), handler.lstrip('/').rstrip('/'))
        self.timeout = timeout
        self.name = name
        self.headers = self._create_headers(content_type)
        self.clear_command()

    def clear_command(self):
        """
        Clear the command. This can be done after command execution to reuse
        the same instance.
        """
        self._commands = []

    @property
    def data(self):
        """
        The data that is posted to the remote host, and must be implemented
        by all sub classes.
        """
        raise NotImplementedError

    def execute(self, return_name=False):
        """
        Execute the command against the Solr instance, returning either the
        response as a JSON-parsed dict or a tuple with the JSON_parsed dict
        and the command name.
        """
        response = None
        try:
            response = self.pool.urlopen('POST', self.url, body=self.data,
                headers=self.headers, timeout=self.timeout,
                assert_same_host=False)
            if response.status == 200:
                json_resp = json.loads(response.data)
                if return_name:
                    return json_resp, self.name
                else:
                    return json_resp
            else:
                raise StellrError(response.reason, url=self.url,
                    body=self.data, response=response.data,
                    status=response.status)
        except StellrError:
            raise
        except urllib3.exceptions.TimeoutError:
            msg = 'Request timed out after %s seconds.' % self.timeout
            raise StellrError(msg, url=self.url, body=self.data, timeout=True)
        except Exception as e:
            data = None if response is None else response.data
            raise StellrError('Error: %s' % e, url=self.url, body=self.data,
                response=data)

    def _create_headers(self, content_type):
        """
        Creates the headers for the request.
        """
        headers = urllib3.connectionpool.make_headers(keep_alive=True)
        headers['content-type'] = content_type
        return headers

class UpdateCommand(BaseCommand):
    """
    An UpdateCommand is used to submit updates to the remote host, and has
    the following initialization parameters:

        host: the solr host the command will be executed against.
        handler: the handler on the remote host that will be called
            (default='/solr/update/json')
        name: the name of the command (default='Update')
        timeout: the timeout of the call to the host in seconds (default=15)
        commit_within: integer value to use as the value to use for the number
            of milliseconds within the documents will be committed
            (default=None)
        commit: boolean value to indicate whether a commit will be performed
            after the documents in the command are added (default=False)

    An UpdateCommand holds a list of commands that are performed in sequence
    on the remote host.
    """

    def __init__(self, host, handler='/solr/update/json', name='update',
                 timeout=DEFAULT_TIMEOUT, commit_within=None, commit=False):
        super(UpdateCommand, self).__init__(
            host, handler, timeout, name, CONTENT_JSON)
        if commit_within is not None:
            self.url += '&commitWithin=%s' % commit_within
        if commit:
            self.url += '&commit=true'

    @property
    def data(self):
        """
        The data posted to the remote host in the format specified at
        http://wiki.apache.org/solr/UpdateJSON. Duplicate names are valid JSON
        (http://www.ietf.org/rfc/rfc4627.txt section 2.2) but not in a
        dictionary.
        """
        writer = StringIO()
        writer.write('{')
        for i in range(len(self._commands)):
            command, document = self._commands[i]
            body = json.dumps(document, cls=StellrJSONEncoder)
            writer.write('"%s": %s' % (command, body))
            if i != len(self._commands) - 1:
                writer.write(',')
        writer.write('}')
        return writer.getvalue()

    def add_documents(self, data, boost=None, overwrite=None):
        """
        Add a document or list of documents to the command that will be added
        to or updated in the index to be updated in the index. The value of
        the single parameter can be one of several things:

            1) dictionary: the keys are taken as field names with the
                corresponding values being the field values (lists are
                acceptable values for multi-valued fields)
            2) object: any valid object p the keys of the objects __dict__
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

        Note about overwrite parameter...

        For fields that make use of the date and time, a datetime instance
        will be correctly submitted to Solr, accurate to the second.
        """
        if isinstance(data, dict):
            self._append_update(data, boost, overwrite)
        elif isinstance(data, list):
            for doc in data:
                self._append_update(doc, boost, overwrite)
        else:
            self._append_update(data, boost, overwrite)

    def _append_update(self, doc, boost, overwrite):
        # if an object, set the doc to its fields
        if hasattr(doc, '__dict__'):
            doc = doc.__dict__
        data = {'doc': doc}
        if boost is not None:
            data['boost'] = boost
        if overwrite is not None:
            data['overwrite'] = overwrite
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
        # append the actual delete
        self._commands.append(('delete', {delete_type: str(data)}))

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
    subclass it for specific types of requests. A SelectCommand has the
    following initialization parameters:

        host: the solr host the command will be executed against.
        handler: the handler on the remote host that will be called
            (default='/solr/update/json')
        name: the name of the command (default='Update')
        timeout: the timeout of the call to the host in seconds (default=15)
    """
    def __init__(self, host, handler='/solr/select', name='select',
                 timeout=DEFAULT_TIMEOUT):
        super(SelectCommand, self).__init__(
            host, handler, timeout, name, CONTENT_FORM)

    def add_param(self, name, value):
        """
        Add a named parameter to the command. Names do not have to be unique
        and can be added multiple times.
        """
        #TODO: is this correct? appears to be double encoding?
        value = unicode(value)
        self._commands.append((name, value.encode('utf-8')))

    @property
    def data(self):
        """
        The data that is posted to the remote host as a string of url encoded
        string of key=value pairs delimited by &.
        """
        return urllib.urlencode(self._commands)

class StellrJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that encodes datetime instances into the UTC format
    expected by Solr: YYYY-MM-DDTHH:MM:SSZ. Date formatting precision is only
    to the second.
    """

    def default(self, o):
        """
        Encode! The datetime instance is expected to be in UTC.
        """
        if isinstance(o, datetime.datetime):
            return o.strftime('%Y-%m-%dT%H:%M:%SZ')
        return json.JSONEncoder.default(self, o)