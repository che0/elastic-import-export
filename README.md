elastic-import-export
=====================

A tool to import your Elasticsearch index data from one-JSON-per-line file, and export the indices to the same format.


```
usage: elastic_import_export.py [-h] [--cacert CACERT] [--cert CERT] [--key KEY]
                                [--doc-type DOC_TYPE] [--verbose]
                                host {import,export} index

Imports or exports data from Elasticsearch index as JSON

positional arguments:
  host                 Elasticsearch host to connect to, optionally with :port
  {import,export}      What to do
  index                Index to operate on

optional arguments:
  -h, --help           show this help message and exit
  --cacert CACERT      Path to CA certificate for Elasticsearch
  --cert CERT          Path to client certificate for Elasticsearch
  --key KEY            Path to client key for Elasticsearch
  --doc-type DOC_TYPE  doc_type for loading document into older Elasticsearch versions
  --verbose, -v        A bit more verbose logging
```
