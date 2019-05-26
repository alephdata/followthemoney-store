from balkhash import settings


def init(name, backend=settings.BACKEND, **config):
    backend = backend.upper()
    assert backend in settings.BACKENDS, \
        "Please specify a supported backend."
    config['name'] = name
    config['backend'] = backend
    if backend == "POSTGRESQL":
        from balkhash.postgres import PostgresDataset
        return PostgresDataset(config)
    elif backend == "DATASTORE":
        from balkhash.datastore import GoogleDatastoreDataset
        return GoogleDatastoreDataset(config)
    else:
        from balkhash.leveldb import LevelDBDataset
        return LevelDBDataset(config)
