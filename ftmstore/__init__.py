from ftmstore.dataset import Dataset


__all__ = ['Dataset']


def init(name, **config):
    return Dataset(name, **config)
