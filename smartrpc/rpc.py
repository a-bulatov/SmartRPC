from typing import Callable, Any, Union
from .env import BaseEnv
from collections import OrderedDict
from .err import *
from .methods import RPCBase, PythonMethod, SQLMethod
import json


class RPC(RPCBase):

    def python_method(self, name: str = None,
                      mapping: Union[dict, int, Callable[[dict], Union[tuple, dict]]] = RPCBase.MAP_ARGS):
        """
        Декоратор для регистрации функций
        :param name: имя метода в АПИ. если не задано, то будет использовано имя функции
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

        if callable(name):
            self.add_python_method(name)
        else:
            return decorator

    def add_python_method(self, func: Union[Callable[..., Any], str], name: str = None,
                          mapping: Union[dict, int, Callable[[dict], Union[tuple, dict]]] = RPCBase.MAP_ARGS):
        if isinstance(func, str):
            # todo тут подгрузить модуль и функцию по имени
            pass
        if name is None:
            name = func.__name__
        self.__methods[name] = PythonMethod(self, func, mapping)

    def sql_method(self, name: str = None, alias: Union[str, dict] = None,
                       mapping: Union[dict, int, Callable[[dict], Union[tuple, dict]]] = RPCBase.MAP_ARGS,
                       postproc = None):
        """
        Декоратор для регистрации SQL. Декорируемая функция должна вернуть строку с запросом, либо словарь,
        содержащий те же параметры, что у декоратора + обязательный параметр query. Функция выполняется только
        раз - при выполнеении декоратора
        :param name: имя метода в АПИ. если не задано, то будет использовано имя декорируемой функции
        :param alias: имя или параметры соединения по умолчанию
        :param mapping: описание мапинга параметров, если нужно
        :param postproc: процедура постобработки данных. на входе получит выборку данных и описание полей
        :return:
        """
        def decorator(fnc):
            query = fnc()
            args = {'rpc': self}
            if isinstance(query, dict):
                fn = query.get('name', name)
                args['alias'] = query.get('alias', alias)
                args['mapping'] = query.get('mapping', mapping)
                args['postproc'] = query.get('postproc', postproc)
                args['query'] = query['query']
            else:
                fn =  name
                args['alias'] =  alias
                args['mapping'] = mapping
                args['postproc'] = postproc
                args['query'] = query
            if fn is None:
                fn = fnc.__name__
            self.__methods[fn] = SQLMethod(**args)

        if callable(name):
            query = name()
            args = {'rpc': self}
            if isinstance(query, dict):
                fn = query.get('name')
                args['alias'] = query.get('alias')
                args['mapping'] = query.get('mapping')
                args['postproc'] = query.get('postproc')
                args['query'] = query['query']
            else:
                fn = None
                args['alias'] = None
                args['mapping'] = RPCBase.MAP_ARGS
                args['postproc'] = None
                args['query'] = query
            if fn is None:
                fn = name.__name__
            self.__methods[fn] = SQLMethod(**args)
        else:
            return decorator

    def add_sql_method(self, name: str, query: str, alias: Union[str, dict] = None,
                       mapping: Union[dict, int, Callable[[dict], Union[tuple, dict]]] = RPCBase.MAP_ARGS,
                       postproc = None):
        self.__methods[name] = SQLMethod(self, query, alias, mapping, postproc)

    def __init__(self, env: BaseEnv = None, simple_format=False):
        RPCBase.__init__(self, env)
        self.__methods = {}
        self.__simple = simple_format
        self.__env = env if env else BaseEnv()

    def __call__(self, *args, **kwargs) -> dict:
        is_func = True
        if len(args) == 1 and kwargs == {}:
            is_func = False
            message = args[0]
        elif len(args) > 1:
            message = {
                'jsonrpc': '2.0',
                'method': args[0],
                'params': args[1:]
            }
        else:
            message = {
                'jsonrpc': '2.0',
                'method': args[0],
                'params': kwargs
            }

        try:
            if isinstance(message, str):
                message = message.strip()
                if not message.startswith('{') and message in self.__methods:
                    message = {
                        "jsonrpc": "2.0",
                        "method": message
                    }
                    is_func = True
                else:
                    message = json.loads(message, object_pairs_hook=OrderedDict)
            if not isinstance(message, dict):
                raise Exception('Parse error')
        except Exception as e:
            if is_func:
                raise e
            else:
                return error(ERR_PARSE)

        f = message.get('method')

        if ((not self.__simple) and str(message.get('jsonrpc', '')) != "2.0") or not isinstance(f, str):
            return error(ERR_REQUEST)

        f = self.__methods.get(f)
        if not f:
            if is_func:
                raise ErrorRPC(ERR_NOT_FOUND)
            else:
                return error(ERR_NOT_FOUND)

        try:
            if callable(f.mapping):
                f = f(message)
            else:
                p = message.get('params')
                if isinstance(p, list) and p != []:
                    f = f(*p)
                elif isinstance(p, dict) and p != {}:
                    f = f(**p)
                else:
                    f = f()

            if is_func:
                return f

            return {
                "jsonrpc": "2.0",
                "result": f,
                "id": message.get('id'),
                "error": None
            }
        except Exception as e:
            if is_func:
                raise e
            if isinstance(e, ErrorRPC):
                return e.message(message.get('id'))
            x = '\n'.join((str(x) for x in e.args))
            if isinstance(e, TypeError):
                return error(ERR_BAD_PARAMS)
            else:
                return error(ERR_INTERNAL, e.__class__.__name__ + ': ' + x)

    @property
    def env(self) -> BaseEnv:
        return self.__env