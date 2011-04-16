A Python API for Solr that supports non-blocking calls made through Tornado.

Requires Python 2.6 for json std lib.
Requires httplib2 for blocking calls or Tornado for non-blocking calls.

Works with Solr 3.1+ as updates use the JSON update handler http://wiki.apache.org/solr/UpdateJSON.

Field or document boosting is not yet supported.