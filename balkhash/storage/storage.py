import abc

from normality import slugify


class Storage(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, bucket_postfix=None):
        self. bucket_postfix = bucket_postfix

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
        if self.bucket_postfix:
            bucketname = "{0}-{1}".format(dataset_name, self.bucket_postfix)
        else:
            bucketname = dataset_name
        bucketname = slugify(bucketname)
        return bucketname
