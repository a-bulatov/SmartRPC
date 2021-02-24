from .env import BaseEnv
from .err import *
import abc


class RPCBase:
    # варианты мапинга входных параметров
    MAP_JSON = 0  # передача пришедших параметров в первый и единственный аргумент функции
    MAP_ARGS = 1  # передача пришедших параметров как args или kwargs зависимости от того list на входе или dict
    MAP_JSON_ENV = 2  # в первый параметр - параметры как пришли. во второй - ссылка на окружение

    def __init__(self, env: BaseEnv = None):
        self.__env = env

    @property
    def env(self) -> BaseEnv:
        return self.__env


class Method:

    def __init__(self, rpc: RPCBase, mapping):
        self.__rpc = rpc
        self.__map = mapping
        self._max_to = -1
        if isinstance(self.__map, dict):
            for n, x in enumerate(self.__map):
                param = self.__map[x]
                param['to'] = param.get('to', x)

                if isinstance(param['to'], int):
                    if n > 0 and self._max_to < 0:
                        raise ErrorRPC(ERR_PARSE, '+Bad map for ' + x)
                    if self._max_to < param['to']:
                        self._max_to = param['to']
                else:
                    if self._max_to >= 0:
                        raise ErrorRPC(ERR_PARSE, '+Bad map for ' + x)
                self.__map[x] = param

    def map(self, args, kwargs):
        if isinstance(self.__map, int):
            return self.__map
        elif callable(self.__map):
            return self.__map(args[0])
        if self._max_to < 0:
            args = {}
        else:
            args = [None] * (self._max_to + 1)
        for x in self.__map:
            f = self.__map[x]
            args[self.__map[x]['to']] = kwargs.get(x, self.__map[x].get('default'))
        return args

    @property
    def mapping(self):
        return self.__map

    @property
    def rpc(self):
        return self.__rpc


class PythonMethod(Method):

    def __init__(self, rpc: RPCBase, func, mapping):
        Method.__init__(self, rpc, mapping)
        self.__func = func

    def __call__(self, *args, **kwargs):
        map = self.map(args, kwargs)
        if map == RPCBase.MAP_ARGS:
            return self.__func(*args, **kwargs)
        elif map == RPCBase.MAP_JSON:
            if args != ():
                return self.__func(args)
            else:
                return self.__func(kwargs)
        elif map == RPCBase.MAP_JSON_ENV:
            if args != ():
                return self.__func(args, self.__rpc.env)
            else:
                return self.__func(kwargs, self.__rpc.env)
        else:
            if isinstance(map, dict):
                return self.__func(**map)
            else:
                return self.__func(*map)


class nBaseSQL:

    def __init__(self, prev_node, data=None):
        self.__prev = prev_node
        self.__next = None
        self.__data = data

    def add_next(self, cls, data=None):
        if cls in nBaseSQL.__subclasses__():
            self.__next = cls(None if self.__class__.__name__ == 'nBaseSQL' else self, data)
            return self.__next

    @property
    def next(self):
        return self.__next

    @property
    def data(self):
        return self.__data

    @abc.abstractmethod
    def run(self, sql_method, local_env):
        pass

    def __call__(self, sql_method, local_env):
        x = self.run(sql_method, local_env)
        if isinstance(x, nBaseSQL):
            return x(sql_method, local_env)
        elif self.next:
            local_env['result'] = x
            return self.next(sql_method, local_env)
        else:
            local_env['result'] = x
            return x


class nAlias(nBaseSQL):

    def run(self, sql_method, local_env):
        aliases = local_env.get('aliases', {})
        alias = aliases.get(self.data)
        if not alias:
            alias = sql_method.rpc.env.connect(self.data)
            aliases[self.data] = alias
            local_env['alias'] = alias


class nSQL(nBaseSQL):

    def run(self, sql_method, local_env):
        alias = local_env.get('alias', local_env['aliases'].get(None))
        if not alias:
            alias = sql_method.rpc.env.connect(self.data)
            local_env['aliases'][None] = alias
        args = local_env.get('args',())
        kwargs = local_env.get('kwargs', {})
        d, f = alias.sql(self.data, *args, **kwargs)
        ret = []
        if f:
            for x in d:
                rec = {f[n]['name']: v for n, v in enumerate(x)}
                ret.append(rec)
        else:
            ret = d
        return ret


class SQLMethod(Method):

    def __init__(self, rpc: RPCBase, query, alias, mapping, postproc=None):
        Method.__init__(self, rpc, mapping)
        pool = {}

        def newnode(pool, cls, data=None):
            last = pool.get('last')
            if last:
                last = last.add_next(cls, data)
            else:
                last = nBaseSQL(None)
                last = last.add_next(cls, data)
                pool['nodes'] = last
            pool['last'] = last

        def addnode(pool, cmd):
            cmd = cmd[1:]
            newnode(pool, nAlias,cmd)
            return True # не ждем @@

        def addquery(pool, qry):
            qry = qry.strip()
            if qry.strip() == '': return
            newnode(pool, nSQL, qry)

        ###################################################
        txt, wait, nextn, newline, ext = '', '', -1, True, ''
        query += '\n'
        for n, x in enumerate(query):
            if n == len(query) - 1: break
            if n == nextn: continue
            if wait != '':
                if wait == "'":
                    txt += x
                    if x == wait: wait = ''
                elif wait == "\n":
                    if x == wait:
                        wait = ''
                        newline = True
                        if ext:
                            if addnode(pool, ext):
                                ext = ''
                            else:
                                wait = '@@'
                    elif ext != '':
                        ext += x
                    continue
                elif wait == '*/' and x == '/' and query[n - 1] == '*':
                    wait = ''
            else:
                if newline and x == '@' and query[n + 1] == '@':
                    nextn = n + 1
                    wait = '\n'
                    ext = '@'
                    if txt:
                        addquery(pool, txt)
                        txt = ''
                elif x == '/' and query[n + 1] == '*':
                    nextn = n + 1
                    wait = '*/'
                elif x == '-' and query[n + 1] == '-':
                    nextn = n + 1
                    wait = '\n'
                elif x in ('\t', '\f', ' ') and newline:
                    continue
                else:
                    newline = x == '\n'
                    txt += x
                    continue
        if ext:
            addnode(pool, ext)
        elif txt:
            addquery(pool, txt)
        self.__nodes = pool.get('nodes')

    def __call__(self, *args, **kwargs):
        local_env = {
            'alias': None,
            'aliases': {},
            'result': None,
            'args': args,
            'kwargs': kwargs
        }
        ret = self.__nodes(self, local_env)

        for x in local_env['aliases']:
            local_env['aliases'][x].commit()

        return ret