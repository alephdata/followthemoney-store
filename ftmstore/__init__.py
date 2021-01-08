from ftmstore.store import Store
from ftmstore.dataset import Dataset
from ftmstore.utils import NULL_ORIGIN


__all__ = ["Dataset", "Store"]


def init(name, origin=NULL_ORIGIN, **config):
    return Store(**config).get(name, origin=origin)
