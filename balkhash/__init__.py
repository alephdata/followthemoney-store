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
    else:
        from balkhash.leveldb import LevelDBDataset
        return LevelDBDataset(config)
