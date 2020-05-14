from balkhash.dataset import Dataset


def init(name, **config):
    config['name'] = name
    return Dataset(config)
