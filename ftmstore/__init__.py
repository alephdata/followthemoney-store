from functools import lru_cache

from ftmstore import settings
from ftmstore.store import Store
from ftmstore.dataset import Dataset
from ftmstore.utils import NULL_ORIGIN


__version__ = "3.1.0"
__all__ = ["Dataset", "Store", "get_store", "get_dataset"]


@lru_cache(maxsize=6)
def get_store(database_uri, **config):
    return Store(database_uri=database_uri, **config)


@lru_cache(maxsize=128)
def get_dataset(name, origin=NULL_ORIGIN, database_uri=None, **config):
    uri = database_uri or settings.DATABASE_URI
    store = get_store(uri, **config)
    return store.get(name, origin=origin)


def init(name, **kwargs):
    return get_dataset(name, **kwargs)
