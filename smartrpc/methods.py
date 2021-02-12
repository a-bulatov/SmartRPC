from .env import BaseEnv
from .err import *
import typing

class RPCBase:
    # варианты мапинга входных параметров
    MAP_JSON = 0  # передача пришедших параметров в первый и единственный аргумент функции
    MAP_ARGS = 1  # передача пришедших параметров как args или kwargs зависимости от того list на входе или dict
    MAP_JSON_ENV = 2  # в первый параметр - параметры как пришли. во второй - ссылка на окружение

    def __init__(self, env:BaseEnv = None):
        self.__env = env

    @property
    def env(self)->BaseEnv:
        return self.__env

class Method:

    def __init__(self, rpc: RPCBase, mapping:typing.Union[dict, int]=RPCBase.MAP_ARGS):
        self.__rpc = rpc
        self.__map = mapping
        self._max_to = -1
        if isinstance(self.__map,dict):
            for n, x in enumerate(self.__map):
                param = self.__map[x]
                param['to'] = param.get('to', x)

                if isinstance(param['to'], int):
                    if n>0 and self._max_to<0:
                        raise ErrorRPC(ERR_PARSE, '+Bad map for ' + x)
                    if self._max_to<param['to']:
                        self._max_to = param['to']
                else:
                    if self._max_to>=0:
                        raise ErrorRPC(ERR_PARSE, '+Bad map for ' + x)
                self.__map[x] = param

    def map(self, kwargs):
       if isinstance(self.__map, int):
           return self.__map
       if self._max_to < 0:
           args = {}
       else:
           args = [None] * (self._max_to + 1)
       for x in self.__map:
           f = self.__map[x]
           args[self.__map[x]['to']] = kwargs.get(x,self.__map[x].get('default'))
       return args

class PythonMethod(Method):

    def __init__(self,  rpc: RPCBase, func: typing.Callable[...,typing.Any], mapping:typing.Union[dict, int]=RPCBase.MAP_ARGS):
        Method.__init__(self, rpc, mapping)
        self.__func = func

    def __call__(self, *args, **kwargs):
        map = self.map(kwargs)
        if map == RPCBase.MAP_ARGS:
            return self.__func(*args, **kwargs)
        elif map == RPCBase.MAP_JSON:
            if args!=():
                return self.__func(args)
            else:
                return self.__func(kwargs)
        elif map == RPCBase.MAP_JSON_ENV:
            if args!=():
                return self.__func(args, self.__rpc.env)
            else:
                return self.__func(kwargs, self.__rpc.env)
        else:
            if isinstance(map, dict):
                return self.__func(**map)
            else:
                return self.__func(*map)

class SQLMethob(Method):
    pass