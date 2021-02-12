from abc import abstractclassmethod
from .db import DBAdapter

class BaseEnv:

    def __init__(self):
        self.aliases = {}

    def set_db_alias(self, name, type='sqlite3', *args, **kwargs):
        a = DBAdapter.get(type, *args, **kwargs)