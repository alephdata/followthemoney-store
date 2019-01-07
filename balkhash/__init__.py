from balkhash.storage import LevelDBStorage


def init():
    storage = LevelDBStorage()
    return storage
