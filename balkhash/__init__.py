from balkhash.leveldb import LevelDBDataset
from balkhash.datastore import GoogleDatastoreDataset


def init(name, remote=False):
    if remote:
        return GoogleDatastoreDataset(name)
    return LevelDBDataset(name)
