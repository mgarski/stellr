stellr
======

A Python API for Solr that supports non-blocking calls made running in a Tornado application as well as blocking calls. The command objects provide full access to all of the parameters of an update or query and the JSON response from Solr is parsed into a nested dictionary.

Requirements
------------

* Developed and tested on Python 2.6.
* Uses simplejson and falls back to the standard library's json module if it is not available.
* Use of the JSON update handler http://wiki.apache.org/solr/UpdateJSON is required with stellr.
* The following packages are required for running the unit tests: tornado, eventlet, zmq, nose, & mock.

Notes
-----
* All calls to Solr are made with the parameters wt=json with the response parsed by the standard library's json module.
* A timeout in seconds may be set on each call, defaulting to 30 seconds. If a timeout is encountered the timeout property on the StellrError raised will be True.
* Datetime instances in an UpdateCommand field are encoded in the format expected by Solr with precision in seconds.

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

    conn = stellr.EventletConnection(<hostname>)
    query = stellr.SelectCommand(handler='/query')
    query.add_param('q', 'a')

    try:
        green_thread = pool.spawn(conn.execute(query))
        response = green_thread.wait()
    except stellr.StellrError as e:
        # handle appropriately

### ZeroMQ w/Eventlet Non-Blocking Calls

The ZeroMQConnection constructor will raise a StellrError if the zmq module cannot be imported from eventlet.green.

    pool = eventlet.GreenPool()

    conn = stellr.ZeroMQConnection(<hostname>)
    query = stellr.SelectCommand(handler='/query')
    query.add_param('q', 'a')

    try:
        green_thread = pool.spawn(conn.execute(query))
        response = green_thread.wait()
    except stellr.StellrError as e:
        # handle appropriately

The ZeroMQConnection sends the command to a ZeroMQ endpoint formatted with the handler and post data delimted by a space.
The handler should not be prefaced with '/solr', but with the name of the core only. If HTTP requests are sent to the
URL http://localhost/solr/core/search?wt=json, with the post body being q=test&rows=12, the ZeroMQ endpoint expects
the string "/core/search?wt=json q=test&rows=12".

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

NOTE: I no longer use the TornadoConnection and will not be completing the unit tests for it however I will
gladly accept a pull request that completes the tests.
