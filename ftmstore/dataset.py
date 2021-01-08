import logging
from datetime import datetime
from normality import slugify
from banal import ensure_list
from followthemoney import model
from followthemoney.proxy import EntityProxy
from sqlalchemy import Column, DateTime, String, UniqueConstraint
from sqlalchemy import Table, JSON
from sqlalchemy import select, distinct, func
from sqlalchemy.dialects.postgresql import JSONB

from ftmstore.loader import BulkLoader
from ftmstore.utils import NULL_ORIGIN

log = logging.getLogger(__name__)


class Dataset(object):
    def __init__(self, store, name, origin=NULL_ORIGIN):
        self.store = store
        self.name = name
        self.origin = origin
        self._table = None

    @property
    def table(self):
        if self._table is not None:
            return self._table
        table_name = slugify("%s %s" % (self.store.prefix, self.name), sep="_")
        json_type = JSONB if self.store.is_postgres else JSON
        self._table = Table(
            table_name,
            self.store.meta,
            Column("id", String, nullable=False),
            Column("origin", String, nullable=False),
            Column("fragment", String, nullable=False),
            Column("timestamp", DateTime, default=datetime.utcnow),
            Column("entity", json_type),
            UniqueConstraint("id", "origin", "fragment"),
            extend_existing=True,
        )
        self._table.create(bind=self.store.engine, checkfirst=True)
        return self._table

    def delete(self, entity_id=None, fragment=None, origin=None):
        table = self.table
        stmt = table.delete()
        if entity_id is not None:
            stmt = stmt.where(table.c.id == entity_id)
        if fragment is not None:
            stmt = stmt.where(table.c.fragment == fragment)
        if origin is not None:
            stmt = stmt.where(table.c.origin == origin)
        self.store.engine.execute(stmt)

    def drop(self):
        log.debug("Dropping ftm-store: %s", self.table)
        self.table.drop(self.store.engine)
        self._table = None

    def put(self, entity, fragment=None, origin=None):
        bulk = self.bulk()
        bulk.put(entity, fragment=fragment, origin=origin)
        return bulk.flush()

    def bulk(self, size=1000):
        return BulkLoader(self, size)

    def fragments(self, entity_ids=None, fragment=None):
        stmt = self.table.select()
        entity_ids = ensure_list(entity_ids)
        if len(entity_ids) == 1:
            stmt = stmt.where(self.table.c.id == entity_ids[0])
        if len(entity_ids) > 1:
            stmt = stmt.where(self.table.c.id.in_(entity_ids))
        if fragment is not None:
            stmt = stmt.where(self.table.c.fragment == fragment)
        stmt = stmt.order_by(self.table.c.id)
        # stmt = stmt.order_by(self.table.c.origin)
        # stmt = stmt.order_by(self.table.c.fragment)
        conn = self.store.engine.connect()
        try:
            conn = conn.execution_options(stream_results=True)
            for ent in conn.execute(stmt):
                data = {"id": ent.id, **ent.entity}
                if ent.origin != NULL_ORIGIN:
                    data["origin"] = ent.origin
                yield data
        finally:
            conn.close()

    def partials(self, entity_id=None, skip_errors=False):
        for fragment in self.fragments(entity_ids=entity_id):
            try:
                yield EntityProxy(model, fragment, cleaned=True)
            except Exception:
                if skip_errors:
                    log.exception("Invalid data [%s]: %s", self.name, fragment["id"])
                    continue
                raise

    def iterate(self, entity_id=None, skip_errors=False):
        entity = None
        invalid = None
        fragments = 1
        for partial in self.partials(entity_id=entity_id, skip_errors=skip_errors):
            if partial.id == invalid:
                continue
            if entity is not None:
                if entity.id == partial.id:
                    fragments += 1
                    if fragments % 10000 == 0:
                        log.debug(
                            "[%s:%s] aggregated %d fragments...",
                            entity.schema.name,
                            entity.id,
                            fragments,
                        )
                    try:
                        entity.merge(partial)
                    except Exception:
                        if skip_errors:
                            log.exception(
                                "Invalid merge [%s]: %s", self.name, entity.id
                            )
                            invalid = entity.id
                            entity = None
                            fragments = 1
                            continue
                        raise
                    continue
                yield entity
            entity = partial
            fragments = 1
        if entity is not None:
            yield entity

    def get(self, entity_id):
        for entity in self.iterate(entity_id=entity_id):
            return entity

    def __iter__(self):
        return self.iterate()

    def __len__(self):
        q = select([func.count(distinct(self.table.c.id))])
        return self.store.engine.execute(q).scalar()

    def __repr__(self):
        return "<Dataset(%r, %r)>" % (self.store, self.name)
