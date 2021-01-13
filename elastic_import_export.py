#!/usr/bin/env python3
import argparse
import elasticsearch
from elasticsearch.helpers import scan
import sys
import json
import logging


class Config:
    @staticmethod
    def from_commandline():
        c = Config()
        parser = argparse.ArgumentParser(
            description='Imports or exports data from Elasticsearch index as JSON')
        parser.add_argument('host', help='Elasticsearch host to connect to, optionally with :port')
        parser.add_argument('action', choices=['import', 'export'], help='What to do')
        parser.add_argument('index', help='Index to operate on')
        parser.add_argument('--cacert', help='Path to CA certificate for Elasticsearch')
        parser.add_argument('--cert', help='Path to client certificate for Elasticsearch')
        parser.add_argument('--key', help='Path to client key for Elasticsearch')
        parser.add_argument('--doc-type',
                            help='doc_type for loading document into older Elasticsearch versions')
        parser.add_argument('--verbose', '-v', action='store_true',
                            help='A bit more verbose logging')
        args = parser.parse_args()

        c.elastic = elasticsearch.Elasticsearch(
            hosts=[args.host],
            use_ssl=args.cert is not None,
            timeout=900,
            verify_certs=args.cacert is not None,
            ca_certs=args.cacert,
            client_cert=args.cert,
            client_key=args.key,
        )
        c.index = args.index
        c.action = args.action
        c.doc_type = args.doc_type
        c.verbose = args.verbose
        return c


class BulkIndexer:
    def __init__(self, config):
        self.elastic = config.elastic
        self.index = config.index
        self.doc_type = config.doc_type
        self.bulk_body = ''
        self.batch_items = 0

        self._index_json = json.dumps({'index': {}})

    def add(self, item):
        self.bulk_body += '{}\n{}\n'.format(
            self._index_json, json.dumps(item))
        self.batch_items += 1
        if self.batch_items >= 512:
            self.flush()

    def flush(self):
        if self.batch_items == 0:
            return

        kwargs = {
            'body': self.bulk_body,
            'index': self.index,
        }
        if self.doc_type:
            kwargs['doc_type'] = self.doc_type
        self.elastic.bulk(**kwargs)
        self.bulk_body = ''
        self.batch_items = 0


def import_action(config):
    document_count = 0

    indexer = BulkIndexer(config)
    for line in sys.stdin:
        indexer.add(json.loads(line))
        document_count += 1

    indexer.flush()

    print('Loaded {} documents into {}'.format(document_count, config.index))


def export_action(config):
    for doc in scan(client=config.elastic, index=config.index):
        json.dump(doc['_source'], sys.stdout, separators=(',', ':'))
        sys.stdout.write('\n')


def main():
    config = Config.from_commandline()
    logging.basicConfig(level=logging.INFO if config.verbose else logging.WARNING)

    action = {'import': import_action, 'export': export_action}[config.action]
    action(config)


if __name__ == '__main__':
    main()
