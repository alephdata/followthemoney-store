from ftmstore.dataset import Dataset


def init(name, **config):
    return Dataset(name, **config)
