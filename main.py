# This is a sample Python script.
from smartrpc import RPC, DBAdapter

rpc = RPC()

@rpc.python_method(name='SUB', mapping={'X':{'to':1,'default':2}, 'Y':{'to':0}})
def substract(minued, subtrahed):
    return minued - subtrahed

@rpc.python_method()
def substract(minued, subtrahed):
    return minued - subtrahed


@rpc.python_method(mapping=RPC.MAP_JSON_ENV, name='TEST')
def test(x, env):
    return x


def test():
    x = rpc({
                "jsonrpc": "2.0",
                "method": "substract",
                "params":{"minued":23,"subtrahed":42},
                "id":1
            })
    print(x)

def test2():
    x = rpc({
                "jsonrpc": "2.0",
                "method": "TEST",
                "params":{"minued":23,"subtrahed":42},
                "id":2
            })
    print(x)

def test3():
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

    x = rpc('SUB', X=3, Y=7)
    print(x)

def dbtest():
    for x in DBAdapter.adapters():
        print(x)

    a = DBAdapter.get('sqlite3','identifier.sqlite')

    x = a.sql("insert into test(name)values('zzzzz')")

    x = a.dicts('select * from test where id = ?', (1,))
    print(x)
    print(a.one('select name from test where id = ?', (2,)))

def dbtest2():
    rpc.add_sql_method('test',"""
    @@TEST
    selec 1 from t1
    @@Alias2
    select 2 from t2
    """)

if __name__ == '__main__':
    #test()
    #test2()
    #test3()

    dbtest()

    #dbtest2()
