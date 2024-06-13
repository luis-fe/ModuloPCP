import jaydebeapi
import gc
from contextlib import contextmanager

def Conexao2():
    conn = jaydebeapi.connect(
    'com.intersys.jdbc.CacheDriver',
    'jdbc:Cache://187.32.10.129:1972/CONSISTEM',
    {'user': 'root', 'password': 'ccscache'},
    './connection/CacheDB.jar'
)
    gc.collect()
    return conn

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