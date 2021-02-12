from typing import Type, Callable, Any, Union
from .env import BaseEnv
from collections import OrderedDict
import json


class ErrorRPC(Exception):

    def __init__(self, code, message=None):
        if isinstance(code, tuple):
            if message is None:
                message = code[1]
            elif message.startswith('+'):
                message = code[1] + '. ' + message[1:]
            code = code[0]
        self.__code = code
        self.__message = message
        Exception.__init__(self, message)

    @property
    def code(self):
        return self.__code

    @property
    def args(self):
        return [self.__message,]

    def message(self, id=None):
        return {
            "jsonrpc": "2.0", "result": None,
            "id": id,
            "error": {"code": self.code, "message": self.__message}
        }

ERR_PARSE =     (-32700, "Parse error")
ERR_REQUEST =   (-32600, "Invalid Request")
ERR_NOT_FOUND = (-32601, "Method not found")
ERR_BAD_PARAMS =(-32602, "Invalid params")
ERR_INTERNAL =  (-32603, "Internal error")
ERR_SERVER =    (-32000, "Server error")

ERR_CONNECT =   (-32701, "Connection error")

def error(code=ERR_INTERNAL, message=None, json_msg=None):
    if isinstance(code, tuple):
        if message is None: message = code[1]
        code = code[0]
    return {
        "jsonrpc": "2.0", "result": None,
        "id": json_msg.get('id') if json_msg else None,
        "error": {"code": code, "message": message}
    }


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

    def __init__(self, rpc: RPCBase, mapping:Union[dict, int]=RPCBase.MAP_ARGS):
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

    def __init__(self,  rpc: RPCBase, func: Callable[...,Any], mapping:Union[dict, int]=RPCBase.MAP_ARGS):
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

class RPC(RPCBase):

    def python_method(self, name:str=None, mapping:Union[dict, int]=RPCBase.MAP_ARGS):
        """
        Декоратор для регистрации функций
        :param name: имя метода в АПИ
        :param mapping: маппинг входных параметров если нужен. в формате:
        {
          "имя параметра в АПИ":{"to":"имя или номер параметра в функции","default:"значение по умолчанию"}
        }
        либо одно из значений PythonMethod.MAP_...

        если в to указвн ноинр паратетра, то номера должны быть указаны для всех to
        :return:
        """
        def decorator(fnc):
            self.add_python_method(fnc, name, mapping)
        return decorator

    def add_python_method(self, func: Union[Callable[...,Any], str], name:str=None, mapping:dict=None):
        if isinstance(func, str):
            # тут подгрузить модуль и функцию по имени
            pass
        if name is None:
            name = func.__name__
        self.__methods[name] = PythonMethod(self, func, mapping)

    def __init__(self, env:BaseEnv = None, simple_format=False):
        RPCBase.__init__(self, env)
        self.__methods = {}
        self.__simple = simple_format

    def __call__(self, message:Union[dict, str])->dict:
        try:
            if isinstance(message, str):
                message = json.loads(message, object_pairs_hook=OrderedDict)
            if not isinstance(message, dict):
                raise Exception('Parse error')
        except Exception as e:
            return error(ERR_PARSE)

        f = message.get('method')

        if ((not self.__simple) and str(message.get('jsonrpc','')) != "2.0") or not isinstance(f, str):
            return error(ERR_REQUEST)

        f = self.__methods.get(f)
        if not f:
            return error(ERR_NOT_FOUND)

        p = message.get('params')
        try:
            if isinstance(p, list) and p!=[]:
                f = f(*p)
            elif isinstance(p, dict) and p!={}:
                f = f(**p)
            else:
                f = f()

            return {
                "jsonrpc": "2.0",
                "result": f,
                "id": message.get('id'),
                "error": None
            }
        except Exception as e:
            if isinstance(e, ErrorRPC):
                return e.message(message.get('id'))
            x = '\n'.join((str(x) for x in e.args))
            if isinstance(e, TypeError):
                return error(ERR_BAD_PARAMS)
            else:
                return error(ERR_INTERNAL, e.__class__.__name__+': '+x )