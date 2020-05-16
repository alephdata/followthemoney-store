import logging
from normality import slugify
from datetime import datetime
from banal import ensure_list
from followthemoney import model
from sqlalchemy import Column, DateTime, String, UniqueConstraint
from sqlalchemy import Table, MetaData, JSON
from sqlalchemy import create_engine, select, distinct, func

from balkhash import settings
from balkhash.loader import BulkLoader

log = logging.getLogger(__name__)


class Dataset(object):

    def __init__(self, name, database_uri=settings.DATABASE_URI,
                 prefix=settings.DATABASE_PREFIX, **config):
        self.name = name
        self.prefix = prefix
        self.engine = create_engine(database_uri)
        self.is_postgres = self.engine.dialect.name == 'postgresql'
        meta = MetaData(self.engine)
        table_name = slugify('%s %s' % (self.prefix, self.name), sep='_')
        self.table = Table(table_name, meta,
            Column('id', String),  # noqa
            Column('fragment', String, nullable=False),
            Column('properties', JSON),
            Column('schema', String),
            Column('timestamp', DateTime, default=datetime.utcnow),
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

    def put(self, entity, fragment=None):
        bulk = self.bulk()
        bulk.put(entity, fragment=fragment)
        return bulk.flush()

    def bulk(self, size=1000):
        return BulkLoader(self, size)

    def close(self):
        self.engine.dispose()

    def fragments(self, entity_ids=None, fragment=None):
        stmt = self.table.select()
        if entity_ids is not None:
            entity_ids = ensure_list(entity_ids)
            if len(entity_ids) == 1:
                stmt = stmt.where(self.table.c.id == entity_ids[0])
            else:
                stmt = stmt.where(self.table.c.id.in_(entity_ids))
        if fragment is not None:
            stmt = stmt.where(self.table.c.fragment == fragment)
        stmt = stmt.order_by(self.table.c.id)
        stmt = stmt.order_by(self.table.c.fragment)
        conn = self.engine.connect()
        try:
            conn = conn.execution_options(stream_results=True)
            entities = conn.execute(stmt)
            for ent in entities:
                ent = dict(ent)
                ent.pop('timestamp', None)
                yield ent
        finally:
            conn.close()

    def partials(self, entity_id=None):
        for fragment in self.fragments(entity_ids=entity_id):
            partial = model.get_proxy(fragment)
            partial.context = {}
            yield partial

    def iterate(self, entity_id=None):
        entity = None
        for partial in self.partials(entity_id=entity_id):
            if entity is not None:
                if entity.id == partial.id:
                    entity.merge(partial)
                    continue
                yield entity
            entity = partial
        if entity is not None:
            yield entity

    def get(self, entity_id):
        for entity in self.iterate(entity_id=entity_id):
            return entity

    def __iter__(self):
        return self.iterate()

    def __len__(self):
        q = select([func.count(distinct(self.table.c.id))])
        return self.engine.execute(q).scalar()

    def __repr__(self):
        return '<Dataset(%r, %r)>' % (self.engine, self.name)
