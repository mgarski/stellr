stellr
======

A Python API for Solr that supports non-blocking calls made running in a Tornado application as well as blocking calls. The command objects provide full access to all of the parameters of an update or query and the JSON response from Solr is parsed into a nested dictionary.

Requirements
------------

* Developed and tested on Python 2.6.
* urllib3, >= 1.1 (earlier versions do not raise a timeout properly).
* eventlet, for non-blocking IO
* Uses simplejson and falls back to the standard library's json module if it is not available.
* Use of the JSON update handler http://wiki.apache.org/solr/UpdateJSON is required with stellr.
* The following packages are required for running the unit tests: eventlet, nose, & mock.

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
