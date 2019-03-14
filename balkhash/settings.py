import os

BACKEND = os.getenv('BALKHASH_BACKEND', 'LEVELDB')

LEVELDB_PATH = os.getenv("BALKHASH_LEVELDB_PATH", "balkhashdb")
DATABASE_URI = os.getenv("BALKHASH_DATABASE_URI", "postgresql://postgres@localhost:5432") # noqa