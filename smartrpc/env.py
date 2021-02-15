from abc import abstractclassmethod
from .db import DBAdapter
from .err import *

class BaseEnv:

    def __init__(self):
        self.__aliases = {}

    def set_db_alias(self, alias_name, type='sqlite3', *args, **kwargs):
        if DBAdapter.get(type, *args, **kwargs) is None:
            raise ErrorRPC(ERR_SERVER, 'Bad alias %s %s' % (alias_name, type))
        self.__aliases[alias_name] = {
            "type": type,
            "args": args,
            "kwargs":kwargs
        }

    def check_alias(self, alias_name):
        return alias_name in self.__aliases

    def connect(self, alias_name=None):
        defs = self.__aliases.get(alias_name)
        if defs:
            return DBAdapter.get(defs['type'], *defs['args'])
        if alias_name is None and len(defs.keys()) == 1:
            defs = self.__aliases[tuple(defs.keys())[0]]
            return DBAdapter.get(defs['type'], *defs['args'])
        raise ErrorRPC(ERR_SERVER, 'Bad alias %s' % alias_name)

