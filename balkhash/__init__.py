from balkhash.storage import LevelDBStorage, GoogleDatastoreStorage


def init(remote=False):
    if remote:
        return GoogleDatastoreStorage()
    return LevelDBStorage()
