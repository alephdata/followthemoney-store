from banal import ensure_list
from memorious.settings import DATASTORE_URI

from balkhash.dataset import Dataset


def get_dataset(context):
    name = context.get('dataset', context.crawler.name)
    return Dataset(name, database_uri=DATASTORE_URI)


def balkhash_put(context, data):
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


def _get_entities(context):
    for entity in get_dataset(context):
        yield entity.to_dict()


def aleph_bulkpush(context, data):
    try:
        from alephclient.memorious import get_api
    except ImportError:
        context.log.error("alephclient not installed. Skipping...")
        return
    api = get_api(context)
    if api is not None:
        foreign_id = context.params.get('foreign_id', context.crawler.name)
        collection = api.load_collection_by_foreign_id(foreign_id, {})
        collection_id = collection.get('id')
        entities = _get_entities(context)
        unsafe = context.params.get('unsafe', False)
        force = context.params.get('force', False)
        api.write_entities(collection_id, entities, unsafe=unsafe, force=force)
