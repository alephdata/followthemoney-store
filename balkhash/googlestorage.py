import logging
from google.cloud import storage


log = logging.getLogger(__name__)


def upload_to_bucket(dataset_name, file_name):
    client = storage.Client()
    bucket = client.create_bucket('occrp-data_' + dataset_name)
    blob = bucket.blob('entities/' + file_name)
    blob.upload_from_filename(file_name)
    log.info('[Uploaded %s]' % dataset_name)
