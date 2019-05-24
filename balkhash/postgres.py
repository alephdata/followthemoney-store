import logging
import datetime
from normality import slugify
from sqlalchemy import Column, DateTime, String, UniqueConstraint
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql

from balkhash import settings
from balkhash.dataset import Dataset, Bulk

log = logging.getLogger(__name__)
# We have to cast null fragment values to "" to make the
# UniqueConstraint work
EMPTY = ''


class PostgresDataset(Dataset):

    def __init__(self, name, database_uri=None, prefix=None):
        super(PostgresDataset, self).__init__(name)
        database_uri = database_uri or settings.DATABASE_URI
        prefix = prefix or settings.DATABASE_PREFIX
        name = '%s %s' % (prefix, name)
        name = slugify(name, sep='_')
        self.engine = create_engine(database_uri)
        meta = MetaData(self.engine)
        self.table = Table(name, meta,
            Column('id', String(128)),  # noqa
            Column('fragment', String(128), nullable=False, default=EMPTY),
            Column('properties', postgresql.JSONB),
            Column('schema', String(128)),
            Column('timestamp', DateTime, default=datetime.datetime.utcnow),
            UniqueConstraint('id', 'fragment'),
            extend_existing=True
        )
        self.table.create(bind=self.engine, checkfirst=True)

    def delete(self, entity_id=None, fragment=None):
        with self.engine.begin() as conn:
            table = self.table
            statement = table.delete()
            if entity_id is not None:
                statement = statement.where(table.c.id == entity_id)
                if fragment is not None:
                    statement = statement.where(table.c.fragment == fragment)
            conn.execute(statement)

    def put(self, entity, fragment=None):
        entity = self._entity_dict(entity)
        with self.engine.begin() as conn:
            upsert_statement = insert(self.table).values(
                id=entity['id'],
                fragment=fragment or EMPTY,
                properties=entity["properties"],
                schema=entity["schema"],
            ).on_conflict_do_update(
                index_elements=['id', 'fragment'],
                set_=dict(
                    properties=entity["properties"],
                    schema=entity["schema"],
                )
            )
            return conn.execute(upsert_statement)

    def bulk(self, size=1000):
        return PostgresBulk(self, size)

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
            if ent["fragment"] == EMPTY:
                ent["fragment"] = None
            yield ent

    def close(self):
        self.engine.dispose()


class PostgresBulk(Bulk):

    def flush(self):
        # Bulk insert WILL FAIL if there are duplicate conflicting values
        with self.dataset.engine.begin() as conn:
            values = [
                {
                    "id": ent['id'],
                    "fragment": frag or EMPTY,
                    "properties": ent["properties"],
                    "schema": ent["schema"]
                } for (ent, frag) in self.buffer
            ]
            if not len(values):
                return
            insert_statement = insert(self.dataset.table).values(values)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=['id', 'fragment'],
                set_=dict(
                    properties=insert_statement.excluded.properties,
                    schema=insert_statement.excluded.schema,
                )
            )
            conn.execute(upsert_statement)
