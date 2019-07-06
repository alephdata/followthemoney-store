import time
import random
import logging
import datetime
from normality import slugify
from psycopg2 import DatabaseError
from sqlalchemy.pool import NullPool
from sqlalchemy import Column, DateTime, String, UniqueConstraint
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine, select, distinct, func
from sqlalchemy.dialects import postgresql

from balkhash import settings
from balkhash.dataset import Dataset, Bulk, DEFAULT

log = logging.getLogger(__name__)


class PostgresDataset(Dataset):

    def __init__(self, config):
        super(PostgresDataset, self).__init__(config)
        database_uri = config.get('database_uri', settings.DATABASE_URI)
        prefix = config.get('prefix', settings.DATABASE_PREFIX)
        name = '%s %s' % (prefix, self.name)
        name = slugify(name, sep='_')
        self.engine = create_engine(database_uri, poolclass=NullPool)
        meta = MetaData(self.engine)
        self.table = Table(name, meta,
            Column('id', String),  # noqa
            Column('fragment', String, nullable=False, default=DEFAULT),
            Column('properties', postgresql.JSONB),
            Column('schema', String),
            Column('timestamp', DateTime, default=datetime.datetime.utcnow),
            UniqueConstraint('id', 'fragment'),
            extend_existing=True
        )
        self.table.create(bind=self.engine, checkfirst=True)

    def delete(self, entity_id=None, fragment=None):
        table = self.table
        statement = table.delete()
        if entity_id is not None:
            statement = statement.where(table.c.id == entity_id)
            if fragment is not None:
                statement = statement.where(table.c.fragment == fragment)
        self.engine.execute(statement)

    def put(self, entity, fragment=DEFAULT):
        bulk = self.bulk()
        bulk.put(entity, fragment=fragment or DEFAULT)
        return bulk.flush()

    def bulk(self, size=1000):
        return PostgresBulk(self, size)

    def close(self):
        self.engine.dispose()

    def fragments(self, entity_id=None, fragment=None):
        table = self.table
        statement = table.select()
        if entity_id is not None:
            statement = statement.where(table.c.id == entity_id)
            if fragment is not None:
                statement = statement.where(table.c.fragment == fragment)
        statement = statement.order_by(table.c.id)
        statement = statement.order_by(table.c.fragment)
        conn = self.engine.connect()
        conn = conn.execution_options(stream_results=True)
        entities = conn.execute(statement)
        for ent in entities:
            ent = dict(ent)
            ent.pop('timestamp', None)
            yield ent

    def __len__(self):
        q = select([func.count(distinct(self.table.c.id))])
        return self.engine.execute(q).scalar()

    def __repr__(self):
        return '<PostgresDataset(%r, %r)>' % (self.engine, self.table.name)


class PostgresBulk(Bulk):

    def flush(self):
        if not len(self.buffer):
            return
        values = []
        for (entity_id, fragment), entity in sorted(self.buffer.items()):
            values.append({
                'id': entity_id,
                'fragment': fragment,
                'properties': entity['properties'],
                'schema': entity['schema']
            })
        for attempt in range(10):
            conn = self.dataset.engine.connect()
            tx = conn.begin()
            try:
                istmt = insert(self.dataset.table).values(values)
                stmt = istmt.on_conflict_do_update(
                    index_elements=['id', 'fragment'],
                    set_=dict(
                        properties=istmt.excluded.properties,
                        schema=istmt.excluded.schema,
                    )
                )
                conn.execute(stmt)
                tx.commit()
                return
            except DatabaseError as err:
                tx.rollback()
                log.error("Database error: %s", err)
                time.sleep(attempt + random.random())
