stellr
======

A Python API for Solr that supports non-blocking calls made running in a Tornado application as well as blocking calls. The command objects provide full access to all of the parameters of an update or query and the JSON response from Solr is parsed into a nested dictionary.

Requirements
------------

* Developed and tested on Python 2.6.
* Uses simplejson and falls back to the standard library's json module if it is not available.
* Use of the JSON update handler http://wiki.apache.org/solr/UpdateJSON is required with stellr.
* The following packages are required for running the unit tests: tornado, eventlet, nose, & mock.
* An instance of Solr 3.1+ running with the example schema and data are required to execute the integration tests.

Notes
-----
* All calls to Solr are made with the parameters wt=json with the response parsed by the standard library's json module.
* A timeout in seconds may be set on each call, defaulting to 30 seconds. If a timeout is encountered the timeout property on the StellrError raised will be True.

Usage
-----

### Overview

* Create a connection object
* Create a command object
* Pass the command to the connection, catching any StellrError raised

### Standard Calls (using urllib)

    conn = stellr.StandardConnection(<hostname>)
    query = stellr.SelectCommand(handler='/query')
    query.add_param('q', 'a')

    try:
        response = conn.execute(query)
    except stellr.StellrError as e:
        # handle appropriately

### Eventlet Non-Blocking Calls

The EventletConnection constructor will raise a StellrError if the eventlet module cannot be imported.

    pool = eventlet.GreenPool()

    conn = stellr.StandardConnection(<hostname>)
    query = stellr.SelectCommand(handler='/query')
    query.add_param('q', 'a')

    try:
        green_thread = pool.spawn(conn.execute(query))
        response = green_thread.wait()
    except stellr.StellrError as e:
        # handle appropriately

### Tornado Non-Blocking Calls

The TornadoConnection constructor will raise a StellrError if the tornado modules cannot be imported or the IOLoop is not created or initialized.

In your Tornado handler, follow the pattern below:

    def get(self):
        conn = stellr.connection.TornadoConnection(<hostname>)
        query = stellr.command.SelectCommand(handler='/query')
        query.add_param('q', 'a')
        conn.execute(query, self._handle_response)

     def _handle_response(self, response):
        if response.error:
            # handle appropriately
        else:
            # data is in response.body

To Do
-----
* Connection unit tests with Mock
* Integration tests with Solr's example schema and data
* Setup
