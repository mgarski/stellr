stellr
======

A Python API for Solr that supports non-blocking calls made running in a Tornado application as well as blocking calls. The command objects provide full access to all of the parameters of an update or query and the JSON response from Solr is parsed into a nested dictionary.

Requirements
------------

* Python 2.6 for json library.
* Run in the context of a Tornado IOLoop for non-blocking calls
* The Tornado library is only needed to run the unit tests, the blocking calls will work fine with only the standard library
* Solr 3.1+ as updates use the JSON update handler http://wiki.apache.org/solr/UpdateJSON.


Notes
-----

* All calls to Solr are made with wt=json.
* Basic authentication is supported on all requests.
* A timeout in seconds may be set on each call, defaulting to 30 seconds. If a timeout is encountered the timeout property on the StellrError raised will be True.
* Field or document boosting is not yet supported.

Usage
-----

### Overview

* Create a connection object
* Create a command object
* Pass the command to the connection, catching any StellrError raised

### Blocking Calls

    conn = stellr.BlockingConnection(<hostname>)
    query = stellr.QueryCommand(handler='/query')
    query.add_param('q', 'a')

    try:
        response = conn.execute(query)
    except stellr.StellrError as e:
        # handle appropriately

### Tornado Async Calls

In your Tornado handler, follow the pattern below:

    def get(self):
        conn = stellr.TornadoConnection(<hostname>)
        query = stellr.QueryCommand(handler='/query')
        query.add_param('q', 'a')
        conn.execute(query, self._handle_response)

     def _handle_response(self, response):
        if response.error:
            # handle appropriately
        else:
            # data is in response.body

To Do
-----

Docstrings

