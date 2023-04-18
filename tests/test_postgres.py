from ftmstore import init, settings

settings.DATABASE_URI = "postgresql://postgres:postgres@postgres:5432/postgres"
# settings.DATABASE_URI = "postgresql://aleph:aleph@localhost:15432/aleph"


def test_postgres():
    dataset = init("TEST-US-OFAC")
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
    assert len(dataset) == 3
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.iterate(entity_id="key3"))) == 1

    both = ["key1", "key3"]
    assert len(list(dataset.iterate(entity_id=both))) == 2

    dataset.delete(entity_id="key1")
    assert len(list(dataset.iterate(entity_id="key1"))) == 0

    bulk = dataset.bulk()
    bulk.put(entity1)
    bulk.put(entity1)
    bulk.put(entity1f, fragment="f")
    bulk.flush()
    assert len(list(dataset.iterate(entity_id="key1"))) == 1
    assert len(list(dataset.fragments(entity_ids="key1"))) == 2

    dataset.drop()
    dataset.store.close()
