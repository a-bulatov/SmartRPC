import abc, typing

MODULES = {}


class DBAdapter:

    @classmethod
    def get(cls, module_name: str, *args, **kwargs):
        for x in cls.__subclasses__():
            if x.module_name(x) == module_name:
                return x(*args, **kwargs)

    @classmethod
    def adapters(cls):
        for x in cls.__subclasses__():
            yield x.module_name(x)

    @abc.abstractclassmethod
    def module_name(cls):
        pass

    def prepare_conn_params(self, args, kwargs):
        if len(args) > 0 and isinstance(args[0], dict):
            return args[0]
        return args if args != () else kwargs

    def __init__(self, *args, **kwargs):
        global MODULES
        n = self.__class__.module_name(self.__class__)
        m = MODULES.get(n)
        if not m:
            m = __import__(n, globals())
            MODULES[n] = m
        self.__m = m
        self.__p = self.prepare_conn_params(args, kwargs)
        self.__conn = None

    @property
    def connection(self):
        if not self.__conn:
            if isinstance(self.__p, dict):
                self.__conn = self.__m.connect(**self.__p)
            else:
                self.__conn = self.__m.connect(*self.__p)
        return self.__conn

    def cursor(self, query: str, *args, **kwargs):
        cursor = self.connection.cursor()
        cursor.execute(query, *args, **kwargs)
        return cursor

    @abc.abstractmethod
    def get_fields(self, cursor):
        # вытащить список имен и типов полей
        return []

    def sql(self, query: str, *args, **kwargs):
        try:
            cursor = self.cursor(query, *args, **kwargs)
            return cursor.fetchall(), self.get_fields(cursor)
        except Exception as e:
            self.rollback()
            raise e

    def dicts(self, query: str, *args, **kwargs):
        d, f = self.sql(query, *args, **kwargs)
        ret = []
        if f:
            for x in d:
                rec = {f[n]['name']: v for n, v in enumerate(x)}
                ret.append(rec)
        return ret

    def __call__(self, query: str, *args, **kwargs):
        try:
            cursor = self.cursor(query, *args, **kwargs)
            return cursor.fetchall()
        except Exception as e:
            self.rollback()
            raise e

    def one(self, query: str, *args, **kwargs):
        try:
            cursor = self.cursor(query, *args, **kwargs)
            ret = tuple(cursor.fetchone())
            return ret[0] if len(ret) == 1 else ret
        except Exception as e:
            self.rollback()
            raise e

    def dml(self, query: str, *args, **kwargs):
        try:
            self.cursor(query, *args, **kwargs)
        except Exception as e:
            self.rollback()
            raise e

    def rollback(self):
        if self.__conn:
            try:
                self.__conn.close()
            except Exception as e:
                pass
        self.__conn = None

    def commit(self):
        if self.__conn:
            self.__conn.commit()
            try:
                self.__conn.close()
            except Exception as e:
                pass
            self.__conn = None


class SQLiteAdapter(DBAdapter):

    def cursor(self, query: str, *args, **kwargs):
        if kwargs != {}:
            query = query % kwargs
            kwargs = {}
        cursor = self.connection.cursor()
        cursor.execute(query, *args, **kwargs)
        return cursor

    def module_name(self):
        return 'sqlite3'

    def get_fields(self, cursor):
        if cursor.description is None: return
        return [{"n": n, "name": x[0]} for n, x in enumerate(cursor.description)]

    def prepare_conn_params(self, args, kwargs):
        if args == () and kwargs == {}:
            return [':memory:']
        else:
            return super().prepare_conn_params(args, kwargs)


class PostgreSQLAdapter(DBAdapter):

    def module_name(cls):
        return 'psycopg2'

    def get_fields(self, cursor):
        if cursor.description is None: return
        return [
            {
                "n": n,
                "name": x.name,
                'type_id': x.type_code
            }
            for n, x in enumerate(cursor.description)
        ]

    def cursor(self, query: str, *args, **kwargs):
        if kwargs != {}:
            query = query % kwargs
            kwargs = {}
        cursor = self.connection.cursor()
        cursor.execute(query, *args, **kwargs)
        return cursor


class TarantoolAdapter(DBAdapter):

    def module_name(cls):
        return 'tarantool'

    def cursor(self, query: str, *args, **kwargs):
        if kwargs != {}:
            args = kwargs
        return self.connection.execute(query, args)

    def prepare_conn_params(self, args, kwargs):
        if len(args) > 0 and isinstance(args[0], dict):
            return args[0]
        if args == ():
            if not 'host' in kwargs:
                kwargs['host'] = '127.0.0.1'
            if not 'port' in kwargs:
                kwargs['port'] = 3301
            return kwargs
        return args

    def sql(self, query: str, *args, **kwargs):
        try:
            cursor = self.cursor(query, *args, **kwargs)
            if cursor.data is None:
                return None, None
            else:
                return cursor.data, self.get_fields(cursor)
        except Exception as e:
            self.rollback()
            raise e

    def one(self, query: str, *args, **kwargs):
        try:
            cursor = self.cursor(query, *args, **kwargs)
            ret = cursor.data[0]
            return ret[0] if len(ret) == 1 else ret
        except Exception as e:
            self.rollback()
            raise e

    def get_fields(self, cursor):
        l = tuple(cursor.body.keys())[0]
        return [
            {
                "n": n,
                "name": x[0],
                'type': x[1]
            }
            for n, x in enumerate(cursor.body[l])
        ]

    def commit(self):
        # fake !!
        self.rollback()

