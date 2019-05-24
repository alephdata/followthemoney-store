from balkhash import settings


def init(name, backend=settings.BACKEND, **kwargs):
    backend = backend.upper()
    assert backend in settings.BACKENDS, \
        "Please specify a supported backend."
    if backend == "POSTGRESQL":
        from balkhash.postgres import PostgresDataset
        return PostgresDataset(name, **kwargs)
    elif backend == "DATASTORE":
        from balkhash.datastore import GoogleDatastoreDataset
        return GoogleDatastoreDataset(name, **kwargs)
    else:
        from balkhash.leveldb import LevelDBDataset
        return LevelDBDataset(name, **kwargs)
