import os

BACKENDS = ('POSTGRESQL', 'LEVELDB',)
BACKEND_ENV = os.getenv('BALKHASH_BACKEND')
BACKEND = BACKEND_ENV or 'LEVELDB'

LEVELDB_PATH = os.getenv('BALKHASH_LEVELDB_PATH', 'balkhashdb')
DATABASE_URI = os.getenv('BALKHASH_DATABASE_URI')
DATABASE_PREFIX = os.getenv('BALKHASH_DATABASE_PREFIX', 'balkhash')

VERBOSE = False
