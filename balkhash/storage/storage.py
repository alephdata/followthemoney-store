import abc

from normality import slugify

from balkhash.dataset import Dataset


class Storage(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    @abc.abstractmethod
    def create_dataset(self, name):
        ns_name = self.generate_namespace_name(name)
        client = self.CLIENT_CLASS(ns_name)
        return Dataset(name, client)

    def generate_namespace_name(self, name):
        return slugify(name).encode('utf-8')


class StorageClient(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, namespace):
        self.namespace = namespace

    @abc.abstractmethod
    def get(self, namespace, key, fragment_id=None):
        pass

    @abc.abstractmethod
    def delete(self, namespace, key, fragment_id=None):
        pass

    @abc.abstractmethod
    def put(self, namespace, key, entity, fragment_id=None):
        pass

    @abc.abstractmethod
    def iterate(self, namespace, prefix=None):
        pass
