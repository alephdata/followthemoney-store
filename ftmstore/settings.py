import os

DATABASE_URI = 'sqlite:///followthemoney.store'
DATABASE_URI = os.getenv('BALKHASH_DATABASE_URI', DATABASE_URI)
DATABASE_URI = os.getenv('FTM_STORE_URI', DATABASE_URI)
DATABASE_PREFIX = os.getenv('FTM_STORE_PREFIX', 'ftm')
