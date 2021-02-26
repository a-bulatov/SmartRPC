import unittest
from smartrpc import RPC, DBAdapter

rpc = RPC()

@rpc.python_method(name='SUB', mapping={'X':{'to':1,'default':2}, 'Y':{'to':0}})
def substract(minued, subtrahed):
    return minued - subtrahed

@rpc.python_method
def substract(minued, subtrahed):
    return minued - subtrahed


@rpc.python_method(mapping=RPC.MAP_JSON_ENV, name='TEST')
def test(x, env):
    return [x, env]

@rpc.sql_method
def fn_sql():
    rpc.env.set_db_alias('tSQL', 'sqlite3', 'test_db.sqlite')

    return """
    @@tSQL
    select * from test
    """


class Tests(unittest.TestCase):

    def setUp(self):
        print('\n%s ------------------------------' % self)

    def test_params(self):
        x = rpc({
                    "jsonrpc": "2.0",
                    "method": "substract",
                    "params":{"minued":23,"subtrahed":42},
                    "id":1
                })
        print(x)

    def test_env(self):
        x = rpc({
                    "jsonrpc": "2.0",
                    "method": "TEST",
                    "params":{"minued":23,"subtrahed":42},
                    "id":2
                })
        print(x)


    def test_mapping(self):
        x = rpc({
            "jsonrpc": "2.0",
            "method": "SUB",
            "params":{"X":23,"Y":42},
            "id":2
        })
        print(x)

        x = rpc({
            "jsonrpc": "2.0",
            "method": "SUB",
            "params": {"Y": 42},
            "id": 2
        })
        print(x)

    def test_simple(self):
        x = rpc('SUB', X=3, Y=7)
        print(x)

    def test_db(self):
        for x in DBAdapter.adapters():
            print(x)

        a = DBAdapter.get('sqlite3','test_db.sqlite')

        x = a.sql("insert into test(name)values('zzzzz')")

        x = a.dicts('select * from test where id = ?', (1,))
        print(x)
        print(a.one('select name from test where id = ?', (2,)))


    def test_db_rpc(self):
        rpc.env.set_db_alias('SQL3','sqlite3','test_db.sqlite')

        rpc.add_sql_method('test3s',"""
        @@SQL3
        select * from test where id = %(id)s
        """)
        x = rpc({
            "jsonrpc": "2.0",
            "method": "test3s",
            "params": {"id": 2},
            "id": 2
        })
        print(x)

    @unittest.skip
    def test_tarantool(self):
        a = DBAdapter.get('tarantool', host='10.0.0.219')
        #a.dml("CREATE TABLE table1 (column1 INTEGER PRIMARY KEY, column2 VARCHAR(100))")
        #a.dml("INSERT INTO table1 VALUES (1, 'A')")
        x = a.sql("UPDATE table1 SET column2 = 'B'")
        print(x)
        x = a.sql("SELECT * FROM table1 WHERE column1 = 5")
        a.commit()
        print(x)

    def test_sql_decorator(self):
        x = rpc('fn_sql')
        print(x)

if __name__ == '__main__':
    unittest.main()