from balkhash.storage import GoogleStorage


def init(bucket_postfix=None):
    storage = GoogleStorage(bucket_postfix=bucket_postfix)
    return storage
