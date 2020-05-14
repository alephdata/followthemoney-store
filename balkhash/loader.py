import time
import random
import logging
from datetime import datetime
from sqlalchemy.exc import DatabaseError, DisconnectionError
from sqlalchemy.dialects.postgresql import insert

from balkhash.utils import valid_fragment

log = logging.getLogger(__name__)

EXCEPTIONS = (DatabaseError, DisconnectionError,)
try:
    from psycopg2 import DatabaseError
    EXCEPTIONS = (DatabaseError, *EXCEPTIONS)
except ImportError:
    pass


class BulkLoader(object):

    def __init__(self, dataset, size):
        self.dataset = dataset
        self.size = size
        self.buffer = {}

    def put(self, entity, fragment=None):
        entity = self.dataset._entity_dict(entity)
        fragment = valid_fragment(fragment)
        self.buffer[(entity['id'], fragment)] = entity
        if len(self.buffer) >= self.size:
            self.flush()

    def flush(self):
        self.buffer = {}


# class UpsertLoader(Loader):

#     def flush(self):
#         if not len(self.buffer):
#             return
#         values = []
#         for (entity_id, fragment), entity in sorted(self.buffer.items()):
#             values.append({
#                 'id': entity_id,
#                 'fragment': fragment,
#                 'properties': entity['properties'],
#                 'schema': entity['schema']
#             })
#         for attempt in range(10):
#             conn = self.dataset.engine.connect()
#             tx = conn.begin()
#             try:
#                 istmt = insert(self.dataset.table).values(values)
#                 stmt = istmt.on_conflict_do_update(
#                     index_elements=['id', 'fragment'],
#                     set_=dict(
#                         properties=istmt.excluded.properties,
#                         schema=istmt.excluded.schema,
#                     )
#                 )
#                 conn.execute(stmt)
#                 tx.commit()
#                 self.buffer = {}
#                 return
#             except EXCEPTIONS as err:
#                 tx.rollback()
#                 log.error("Database error: %s", err)
#                 time.sleep(attempt + random.random())
