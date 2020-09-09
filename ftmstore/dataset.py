import logging
from normality import slugify
from datetime import datetime
from banal import ensure_list
from followthemoney import model
from followthemoney.proxy import EntityProxy
from sqlalchemy import Column, DateTime, String, UniqueConstraint
from sqlalchemy import Table, MetaData, JSON
from sqlalchemy import create_engine, select, distinct, func
from sqlalchemy.dialects.postgresql import JSONB

from ftmstore import settings
from ftmstore.loader import BulkLoader
from ftmstore.utils import DroppedException

NULL_ORIGIN = "null"
log = logging.getLogger(__name__)


class Dataset(object):
    def __init__(
        self,
        name,
        origin=NULL_ORIGIN,
        database_uri=settings.DATABASE_URI,
        prefix=settings.DATABASE_PREFIX,
        **config
    ):
        self.name = name
        self.origin = origin
        self.prefix = prefix
        # config.setdefault('pool_size', 1)
        self.engine = create_engine(database_uri, **config)
        self.is_postgres = self.engine.dialect.name == "postgresql"
        meta = MetaData(self.engine)
        table_name = slugify("%s %s" % (self.prefix, self.name), sep="_")
        self.table = Table(
            table_name,
            meta,
            Column("id", String, nullable=False),
            Column("origin", String, nullable=False),
            Column("fragment", String, nullable=False),
            Column("timestamp", DateTime, default=datetime.utcnow),
            Column("entity", JSONB if self.is_postgres else JSON),
            UniqueConstraint("id", "origin", "fragment"),
            extend_existing=True,
        )
        self.table.create(bind=self.engine, checkfirst=True)
        self._dropped = False

    def delete(self, entity_id=None, fragment=None, origin=None):
        if self._dropped:
            raise DroppedException()
        table = self.table
        stmt = table.delete()
        if entity_id is not None:
            stmt = stmt.where(table.c.id == entity_id)
        if fragment is not None:
            stmt = stmt.where(table.c.fragment == fragment)
        if origin is not None:
            stmt = stmt.where(table.c.origin == origin)
        self.engine.execute(stmt)

    def drop(self):
        self._dropped = True
        log.debug("Dropping ftm-store: %s", self.table)
        self.table.drop(self.engine)
        self.close()

    def put(self, entity, fragment=None, origin=None):
        bulk = self.bulk()
        bulk.put(entity, fragment=fragment, origin=origin)
        return bulk.flush()

    def bulk(self, size=1000):
        return BulkLoader(self, size)

    def close(self):
        self.engine.dispose()

    def fragments(self, entity_ids=None, fragment=None):
        if self._dropped:
            raise DroppedException()
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
        conn = self.engine.connect()
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
        if self._dropped:
            raise DroppedException()
        q = select([func.count(distinct(self.table.c.id))])
        return self.engine.execute(q).scalar()

    def __repr__(self):
        return "<Dataset(%r, %r)>" % (self.engine, self.name)
