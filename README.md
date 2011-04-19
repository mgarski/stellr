stellr
======

A Python API for Solr that supports non-blocking calls made running in a Tornado application as well as traditional blocking calls.

Requirements
------------

* Python 2.6 for json library.
* Run in the context of a Tornado IOLoop for non-blocking calls
* The Tornado library is only needed to run the unit tests, the blocking calls will work fine with only the standard library
* Solr 3.1+ as updates use the JSON update handler http://wiki.apache.org/solr/UpdateJSON.


Notes
-----

* Field or document boosting is not yet supported.

Usage
-----

### Overview

Create a connection object
Create a command object
Pass the command to the connection

...

Profit!

### Blocking Calls

An example will be coming, for now check out the test file

### Tornado Async Calls

An example will be coming, for now check out the test file

To Do
-----

Thorough testing of:
    [x] add with dictionary
    [ ] add with list of dictionaries
    [x] add with object
    [x] mixed add
    [ ] add with list of objects
    [ ] delete by id
    [ ] delete by list of ids
    [ ] delete by query
    [ ] delete by list of queries
    [ ] commit
    [ ] optimize
    [x] queries

Docstrings
Unit test
Verify timeout for Tornado request

