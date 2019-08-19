import logging

from banal import ensure_list
from memorious.settings import DATASTORE_URI
from alephclient.api import AlephAPI
from alephclient import settings as alephclient_settings

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
    reader = get_dataset(context)
    entities = reader.iterate()
    for entity in entities:
        yield entity.to_dict()


def aleph_bulk_push(context, params=None):
    if params is None:
        params = {}
    entities = _get_entities(context)
    host = alephclient_settings.HOST
    api_key = alephclient_settings.API_KEY
    retries = alephclient_settings.MAX_TRIES
    api = AlephAPI(host, api_key, retries=retries)
    foreign_id = params.get('foreign_id') or context.crawler.name
    collection = api.load_collection_by_foreign_id(foreign_id, {})
    collection_id = collection.get('id')
    merge = params.get('merge') or False
    unsafe = params.get('unsafe') or False
    force = params.get('force') or False
    api.write_entities(
        collection_id, entities, merge=merge, unsafe=unsafe, force=force
    )
