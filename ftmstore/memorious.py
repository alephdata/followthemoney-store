from banal import ensure_list
from memorious.settings import DATASTORE_URI
from followthemoney import model

from ftmstore.dataset import Dataset
from ftmstore.settings import DATABASE_URI, DEFAULT_DATABASE_URI

ORIGIN = 'memorious'


def get_dataset(context, origin=ORIGIN):
    name = context.get('dataset', context.crawler.name)
    origin = context.get('dataset', origin)
    # Either use a database URI that has been explicitly set as a
    # backend, or default to the memorious datastore.
    database_uri = DATABASE_URI
    if DATABASE_URI == DEFAULT_DATABASE_URI:
        database_uri = DATASTORE_URI
    return Dataset(name, database_uri=database_uri, origin=origin)


def ftm_store(context, data):
    """Store an entity or a list of entities to an ftm store."""
    # This is a simplistic implementation of a balkhash memorious operation.
    # It is meant to serve the use of OCCRP where we pipe data into postgresql.
    writer = get_dataset(context)
    entities = ensure_list(data.get('entities', data))
    for entity in entities:
        context.log.debug("Store entity [%(schema)s]: %(id)s", entity)
        writer.put(entity, entity.pop('fragment', None))
        context.emit(rule='fragment', data=data, optional=True)
    context.emit(data=data, optional=True)
    writer.close()


def ftm_load_aleph(context, data):
    """Write each entity from an ftm store to Aleph via the _bulk API."""
    try:
        from alephclient.memorious import get_api
    except ImportError:
        context.log.error("alephclient not installed. Skipping...")
        return
    api = get_api(context)
    if api is None:
        return
    foreign_id = context.params.get('foreign_id', context.crawler.name)
    collection = api.load_collection_by_foreign_id(foreign_id, {})
    collection_id = collection.get('id')
    unsafe = context.params.get('unsafe', False)
    entities = get_dataset(context)
    api.write_entities(collection_id, entities, unsafe=unsafe)


class EntityEmitter(object):
    """Utility helper for emitting a bunch of entities from a memorious
    crawler."""

    def __init__(self, context, origin=ORIGIN):
        self.fragment = 0
        self.log = context.log
        self.name = context.crawler.name
        self.dataset = get_dataset(context, origin=origin)
        self.bulk = self.dataset.bulk()

    def make(self, schema):
        entity = model.make_entity(schema, key_prefix=self.name)
        return entity

    def emit(self, entity, rule='pass'):
        if entity.id is None:
            raise RuntimeError("Entity has no ID: %r", entity)
        fragment = str(self.fragment)
        self.bulk.put(entity, fragment=fragment)
        self.fragment += 1

    def finalize(self):
        self.bulk.flush()
