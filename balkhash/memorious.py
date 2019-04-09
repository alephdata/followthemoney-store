from banal import ensure_list
from memorious.settings import DATASTORE_URI

from balkhash import settings, init
from balkhash.postgres import PostgresDataset


def get_dataset(context):
    dataset = context.get('dataset', context.crawler.name)
    backend = context.get('backend', settings.BACKEND_ENV)
    if backend is None and 'postgres' in DATASTORE_URI:
        return PostgresDataset(dataset, database_uri=DATASTORE_URI)
    return init(dataset, backend=backend)


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
