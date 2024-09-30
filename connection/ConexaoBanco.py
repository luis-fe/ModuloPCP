import jaydebeapi
from contextlib import contextmanager


@contextmanager
def Conexao2():
    conn = None
    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
            {'user': 'root', 'password': 'ccscache'},
            './connection/CacheDB.jar'
        )
        yield conn
    finally:
        if conn:
            conn.close()
@contextmanager
def ConexaoInternoMPL():
    conn = None
    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            'jdbc:Cache://192.168.0.25:1972/CONSISTEM',
            {'user': '_system', 'password': 'ccscache'},
            './connection/CacheDB.jar'
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()






