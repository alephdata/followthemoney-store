from ftmstore import init


def test_sqlite():
    uri = "sqlite://"
    dataset = init("TEST-US-OFAC", database_uri=uri)
    assert dataset.name == "TEST-US-OFAC"

    entity1 = {"id": "key1", "schema": "Person", "properties": {}}
    entity1f = {"id": "key1", "schema": "LegalEntity", "properties": {}}
    entity2 = {"id": "key2", "schema": "Person", "properties": {}}
    props = {"name": ["Banana Man"]}
    entity3 = {"id": "key3", "schema": "Person", "properties": props}

    dataset.put(entity1)
    dataset.put(entity1f, fragment="f")
    dataset.put(entity2)
    dataset.put(entity3, fragment="2")

    assert dataset.get("key1").schema.name == "Person"

    assert len(list(dataset.iterate())) == 3
    assert len(list(dataset)) == 3
    assert len(dataset) == 3
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.iterate(entity_id="key3"))) == 1

    dataset.close()
