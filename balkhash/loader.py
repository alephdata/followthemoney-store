import time
import random
import logging
from datetime import datetime
from sqlalchemy.exc import DatabaseError, DisconnectionError, IntegrityError
from sqlalchemy.sql.expression import insert, update
from sqlalchemy.dialects.postgresql import insert as upsert

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
        fragment = valid_fragment(fragment)
        self.buffer[(entity['id'], fragment)] = entity
        if len(self.buffer) >= self.size:
            self.flush()

    def _store_values(self, conn, values):
        table = self.dataset.table
        changing = ('properties', 'schema', 'timestamp',)
        try:
            conn.execute(insert(table).values(values))
        except IntegrityError:
            for value in values:
                stmt = update(table)
                changed = {c: value[c] for c in changing}
                stmt = stmt.values(changed)
                stmt = stmt.where(table.c.id == value['id'])
                stmt = stmt.where(table.c.fragment == value['fragment'])
                stmt = stmt.where(table.c.timestamp < value['timestamp'])
                conn.execute(stmt)

    def _upsert_values(self, conn, values):
        istmt = upsert(self.dataset.table).values(values)
        stmt = istmt.on_conflict_do_update(
            index_elements=['id', 'fragment'],
            set_=dict(
                properties=istmt.excluded.properties,
                schema=istmt.excluded.schema,
            )
        )
        conn.execute(stmt)

    def flush(self):
        if not len(self.buffer):
            return
        values = []
        now = datetime.utcnow()
        for (entity_id, fragment), entity in sorted(self.buffer.items()):
            if hasattr(entity, 'to_dict'):
                entity = entity.to_dict()
            values.append({
                'id': entity_id,
                'fragment': fragment,
                'properties': entity['properties'],
                'schema': entity['schema'],
                'timestamp': now
            })
        conn = self.dataset.engine.connect()
        tx = conn.begin()
        self._store_values(conn, values)
        tx.commit()
        conn.close()
        self.buffer = {}

        # for attempt in [1]:
        #     conn = self.dataset.engine.connect()
        #     tx = conn.begin()
        #     try:
        #         self._store_values(conn, values)
        #         tx.commit()
        #         self.buffer = {}
        #         return
        #     except EXCEPTIONS as err:
        #         tx.rollback()
        #         log.error("Database error: %s", err)
        #         time.sleep(attempt + random.random())
