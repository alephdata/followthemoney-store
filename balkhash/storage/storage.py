from abc import ABC, abstractmethod

from normality import slugify

from balkhash.dataset import Dataset


class Storage(ABC):
    @property
    @abstractmethod
    def client_class(self):
        pass

    def create_dataset(self, name):
        ns_name = self.generate_namespace_name(name)
        client = self.client_class(ns_name)
        return Dataset(name, client)

    def generate_namespace_name(self, name):
        return slugify(name).encode('utf-8')


class StorageClient(ABC):
    def __init__(self, namespace):
        self.namespace = namespace

    @abstractmethod
    def get(self, key, fragment_id=None):
        pass

    @abstractmethod
    def delete(self, key, fragment_id=None):
        pass

    @abstractmethod
    def put(self, key, entity, fragment_id=None):
        pass

    @abstractmethod
    def iterate(self, prefix=None):
        pass
