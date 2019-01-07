import shutil

import balkhash


def test_leveldb():
    storage = balkhash.init()
    dataset = storage.create_dataset(name="TEST-US-OFAC", db_path="testdb")
    assert dataset.name == "TEST-US-OFAC"

    dataset.put(b"key1", b"val1")
    dataset.put(b"key3", b"val3")
    dataset.put(b"key1", b"val2", fragment_id=b"2")

    assert dataset.get(b"key1") == b"val1"
    assert dataset.get(b"key1", fragment_id=b"2") == b"val2"

    print(list(dataset.iterate(prefix=b"key1")))
    print(list(dataset.iterate(prefix=None)))
    assert len(list(dataset.iterate())) == 3
    assert len(list(dataset.iterate(prefix=b"key1"))) == 2

    shutil.rmtree("testdb")
