import logging
import json

from google.cloud import datastore

from balkhash.dataset import Dataset
from .storage import Storage


log = logging.getLogger(__name__)


class GoogleDatastoreStorage(Storage):
    def create_dataset(self, name):
        ns_name = self.generate_namespace_name(name)
        namespace = datastore.Client(namespace=ns_name)
        return Dataset(name, self, namespace)

    def get(self, namespace, key):
        key = namespace.key('Entity', key)
        return namespace.get(key)

    def delete(self, namespace, key):
        key = namespace.key('Entity', key)
        return namespace.delete(key)

    def put(self, namespace, key, val):
        key = namespace.key('Entity', key)
        entity = datastore.Entity(key=key)
        entity.update(json.loads(val))
        return namespace.put(entity)

    def iterate(self, namespace, prefix=None):
        query = namespace.query(kind='Entity')
        return query.fetch()
