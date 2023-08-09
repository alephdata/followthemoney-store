from normality import slugify
from sqlalchemy import create_engine, MetaData
from sqlalchemy import inspect as sqlalchemy_inspect

from ftmstore import settings
from ftmstore.dataset import Dataset
from ftmstore.utils import NULL_ORIGIN


class Store(object):
    """A database containing multiple tables that represent
    FtM-store datasets."""

    def __init__(
        self,
        database_uri=settings.DATABASE_URI,
        prefix=settings.DATABASE_PREFIX,
        **config,
    ):
        self.prefix = prefix
        self.database_uri = database_uri
        # config.setdefault('pool_size', 1)
        self.engine = create_engine(database_uri, future=True, **config)
        self.is_postgres = self.engine.dialect.name == "postgresql"
        self.meta = MetaData()

    def get(self, name, origin=NULL_ORIGIN):
        return Dataset(self, name, origin=origin)

    def all(self, origin=NULL_ORIGIN):
        prefix = slugify("%s " % self.prefix, sep="_") + "_"
        inspect = sqlalchemy_inspect(self.engine)
        for table in inspect.get_table_names():
            if table.startswith(prefix):
                name = table[len(prefix) :]
                yield Dataset(self, name, origin=origin)

    def close(self):
        self.engine.dispose()

    def __len__(self):
        return len(list(self.all()))

    def __repr__(self):
        return "<Store(%r)>" % self.engine
