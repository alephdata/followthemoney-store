import abc

from normality import slugify

from balkhash.settings import BUCKET_POSTFIX


class Storage(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def create_dataset(name, public=False):
        pass

    @abc.abstractmethod
    def exists(self, bucket, key):
        pass

    @abc.abstractmethod
    def get(self, bucket, key):
        pass

    @abc.abstractmethod
    def put(self, bucket, key, val):
        pass

    @abc.abstractmethod
    def list_blobs(self, bucket, prefix=None):
        pass

    @abc.abstractmethod
    def get_blob_url(self, bucket, key):
        pass

    @abc.abstractmethod
    def get_bucket_url(self, bucket):
        pass

    def generate_bucketname(self, dataset_name):
        dataset_name = slugify(dataset_name)
        bucketname = "{0}.{1}".format(dataset_name, BUCKET_POSTFIX)
        return bucketname
