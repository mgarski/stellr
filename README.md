stellr
======

A Python API for Solr that supports non-blocking calls using gevent via urllib3 or ZeroMQ.

Requirements
------------

* Developed and tested on Python 2.6 with
** urllib3, >= 1.1 (earlier versions do not raise a timeout properly)
** pyzmq 2.0.10.1
** gevent 0.13.6
** gevent_zeromq 0.2.2
** simplejson 2.1.6
** Use of the JSON update handler http://wiki.apache.org/solr/UpdateJSON is required with stellr.
** Nose and mock are necessary to run the unit tests.

Notes
-----
* All calls to Solr are made with the parameters wt=json with the response parsed by the standard library's json module.
* A timeout in seconds may be set on each call, defaulting to 15 seconds. If a timeout is encountered the timeout property on the StellrError raised will be True.
* Datetime instances in an UpdateCommand field are encoded in the format expected by Solr with precision in seconds.

Usage
-----

### Overview

* Create a command object
* Call the execute method of the command, catching any StellrError raised
