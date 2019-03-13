import logging
import datetime


from sqlalchemy import Column, DateTime, String, UniqueConstraint
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy import and_

from balkhash import settings
from balkhash.dataset import Dataset, Bulk


log = logging.getLogger(__name__)


class PostgresDataset(Dataset):

    def __init__(self, name):
        super(PostgresDataset, self).__init__(name)
        self.engine = create_engine(settings.DATABASE_URI)
        self.table_name = name
        meta = MetaData(self.engine)
        meta.reflect()
        self.table = Table(
            name, meta,
            Column('id', String(60)),
            # We have to cast null fragment values to "" to make the
            # UniqueConstraint work
            Column('fragment', String(60), nullable=False, default=""),
            Column('properties', postgresql.JSONB),
            Column('schema', String(32)),
            Column('timestamp', DateTime, default=datetime.datetime.utcnow),
            UniqueConstraint('id', 'fragment'),
            extend_existing=True
        )
        meta.create_all(self.engine)

    def delete(self, entity_id=None, fragment=None):
        with self.engine.begin() as conn:
            table = self.table
            if entity_id:
                if fragment:
                    statement = table.delete(and_(
                        table.c.id == entity_id,
                        table.c.fragment == fragment
                    ))
                else:
                    statement = table.delete(table.c.id == entity_id)
            conn.execute(statement)

    def put(self, entity, fragment=None):
        with self.engine.begin() as conn:
            upsert_statement = insert(self.table).values(
                id=entity['id'],
                fragment=fragment or "",
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
        with self.engine.begin() as conn:
            table = self.table
            statement = table.select()
            if entity_id:
                if fragment:
                    statement = statement.where(and_(
                        table.c.id == entity_id,
                        table.c.fragment == fragment
                    ))
                else:
                    statement = statement.where(table.c.id == entity_id)
            entities = conn.execute(statement)
            for ent in entities:
                ent = dict(ent)
                if ent["fragment"] == "":
                    ent["fragment"] = None
                yield ent


class PostgresBulk(Bulk):

    def flush(self):
        # Bulk insert WILL FAIL if there are duplicate conflicting values
        with self.dataset.engine.begin() as conn:
            values = [
                {
                    "id": ent['id'],
                    "fragment": frag or "",
                    "properties": ent["properties"],
                    "schema": ent["schema"]
                } for (ent, frag) in self.buffer
            ]
            insert_statement = insert(self.dataset.table).values(values)
            upsert_statement = insert_statement.on_conflict_do_update(
                index_elements=['id', 'fragment'],
                set_=dict(
                    properties=insert_statement.excluded.properties,
                    schema=insert_statement.excluded.schema,
                )
            )
            return conn.execute(upsert_statement)
