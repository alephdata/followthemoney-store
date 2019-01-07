import abc

from normality import slugify


class Storage(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self):
        pass

    @abc.abstractmethod
    def create_dataset(name):
        pass

    @abc.abstractmethod
    def get(self, namespace, key):
        pass

    @abc.abstractmethod
    def delete(self, namespace, key):
        pass

    @abc.abstractmethod
    def put(self, namespace, key, val):
        pass

    @abc.abstractmethod
    def iterate(self, namespace, prefix=None):
        pass

    def generate_namespace_name(self, name):
        return slugify(name).encode('utf-8')
