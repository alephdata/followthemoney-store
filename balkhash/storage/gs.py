import logging
# from datetime import datetime, timedelta
from google.cloud.storage import Blob
from google.cloud.storage.client import Client

from balkhash.dataset import Dataset
from .storage import Storage


log = logging.getLogger(__name__)


class GoogleStorage(Storage):
    TIMEOUT = 84600

    def __init__(self, **kwargs):
        super(GoogleStorage, self).__init__(**kwargs)
        self.client = Client()

    def create_dataset(self, name, public=False):
        bucket_name = self.generate_bucketname(name)
        log.info("Archive: gs://%s", bucket_name)

        bucket = self.client.lookup_bucket(bucket_name)
        if bucket is None:
            bucket = self.client.create_bucket(bucket_name)

        policy = {
            "origin": ['*'],
            "method": ['GET'],
            "responseHeader": [
                'Accept-Ranges',
                'Content-Encoding',
                'Content-Length',
                'Content-Range'
            ],
            "maxAgeSeconds": self.TIMEOUT
        }
        bucket.cors = [policy]
        bucket.update()
        if public:
            bucket.make_public(recursive=True, future=True)
        dataset = Dataset(name, self, bucket)
        return dataset

    def exists(self, bucket, key):
        """Does the key exist in the bucket?

        :param bucket: The bucket instance
        :type bucket: google.cloud.storage.bucket.Bucket
        :param key: The key we're looking for
        :type key: str
        :return: True if blob exists; else False
        :rtype: bool
        """

        blob = Blob(key, bucket)
        return blob.exists()

    def get(self, bucket, key):
        """Get a blob from a bucket by its key.

        :param bucket: The bucket to fetch the key from
        :type bucket: google.cloud.storage.bucket.Bucket
        :param key: The key we are looking for
        :type key: str
        :return: The contents of the blob or None
        :rtype: bytes or None
        """

        blob = bucket.get_blob(key)
        if blob:
            return blob.download_as_string()

    def put(self, bucket, key, val):
        """Store a blob into a bucket

        :param bucket: The bucket instance in which to store the blob
        :type bucket: google.cloud.storage.bucket.Bucket
        :param key: The key to reference the blob
        :type key: str
        :param val: The contents of the blob
        :type val: bytes or str
        """

        blob = Blob(key, bucket)
        blob.upload_from_string(val)

    def list_blobs(self, bucket, prefix=None):
        """Return an iterator of blobs in the bucket with keys starting with
        the given prefix.

        :param bucket: The bucket instance from which blobs are listed
        :type bucket: google.cloud.storage.bucket.Bucket
        :param prefix: We are looking for blobs with keys starting with this
        string
        :type prefix: str
        :return: An iterator over the contents of the blobs
        :rtype: Iterator
        """
        blobs = bucket.list_blobs(prefix=prefix)
        for blob in blobs:
            yield blob.download_as_string()

    def get_blob_url(self, bucket, key):
        blob = Blob(key, bucket)
        return blob.path

    def get_bucket_url(self, bucket):
        return bucket.path
