from balkhash.leveldb import LevelDBDataset
from balkhash.datastore import GoogleDatastoreDataset
from balkhash.postgres import PostgresDataset

BACKENDS = {
    'POSTGRESQL': PostgresDataset,
    'DATASTORE': GoogleDatastoreDataset,
    'LEVELDB': LevelDBDataset,
}


def init(name, backend='LEVELDB', remote=False):
    if remote:
        return GoogleDatastoreDataset(name)
    else:
        assert backend in BACKENDS, "Please specify a supported backend."
        return BACKENDS[backend](name)
