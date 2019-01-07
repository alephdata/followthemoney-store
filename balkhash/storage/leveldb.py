import logging
import os

import plyvel

from balkhash.dataset import Dataset
from .storage import Storage


log = logging.getLogger(__name__)
DB_PATH = os.getenv("BALKHASH_LOCAL_DB", "balkhashdb")


class LevelDBStorage(Storage):
    def __init__(self):
        pass

    def create_dataset(self, name, db_path=None):
        db_path = db_path or DB_PATH
        db = plyvel.DB(db_path, create_if_missing=True)
        ns_name = self.generate_namespace_name(name)
        namespace = db.prefixed_db(ns_name)
        return Dataset(name, self, namespace)

    def get(self, namespace, key):
        return namespace.get(key)

    def delete(self, namespace, key):
        return namespace.delete(key)

    def put(self, namespace, key, val):
        return namespace.put(key, val)

    def iterate(self, namespace, prefix=None):
        return namespace.iterator(prefix=prefix)
