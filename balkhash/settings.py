import os

DATABASE_URI = 'sqlite:///balkhash.sqlite3'
DATABASE_URI = os.getenv('BALKHASH_DATABASE_URI', DATABASE_URI)
DATABASE_PREFIX = os.getenv('BALKHASH_DATABASE_PREFIX', 'balkhash2')
