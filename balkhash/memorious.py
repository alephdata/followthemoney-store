import logging

from banal import ensure_list
from memorious.settings import DATASTORE_URI

from balkhash import settings, init


log = logging.getLogger(__name__)


def get_dataset(context):
    config = {
        'name': context.get('dataset', context.crawler.name),
        'backend': context.get('backend', settings.BACKEND_ENV)
    }
    if config['backend'] is None and 'postgres' in DATASTORE_URI:
        config['backend'] = 'POSTGRESQL'
        config['database_uri'] = DATASTORE_URI
    return init(**config)


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
