BACKENDS = ('POSTGRESQL', 'DATASTORE', 'LEVELDB',)


def init(name, backend='LEVELDB', remote=False):
    if remote:
        from balkhash.datastore import GoogleDatastoreDataset
        return GoogleDatastoreDataset(name)
    else:
        assert backend in BACKENDS, "Please specify a supported backend."
        if backend == "POSTGRESQL":
            from balkhash.postgres import PostgresDataset
            return PostgresDataset(name)
        elif backend == "DATASTORE":
            from balkhash.datastore import GoogleDatastoreDataset
            return GoogleDatastoreDataset(name)
        else:
            from balkhash.leveldb import LevelDBDataset
            return LevelDBDataset(name)
