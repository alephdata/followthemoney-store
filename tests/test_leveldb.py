import shutil

import balkhash

TEST_DB_PATH = 'testdb'


def test_leveldb():
    dataset = balkhash.init("TEST-US-OFAC")
    assert dataset.name == "TEST-US-OFAC"

    entity1 = {"name": "ent1"}
    entity2 = {"name": "ent2"}
    entity3 = {"name": "ent3"}

    dataset.put("key1", entity1)
    dataset.put("key3", entity3)
    dataset.put("key1", entity2, fragment_id="2")

    assert dataset.get("key1") == entity1
    assert dataset.get("key1", fragment_id="2") == entity2

    assert len(list(dataset.iterate())) == 3
    assert len(list(dataset.iterate(prefix="key1"))) == 2

    shutil.rmtree(TEST_DB_PATH)
