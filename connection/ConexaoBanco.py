import jaydebeapi
from contextlib import contextmanager
from dotenv import load_dotenv, dotenv_values
import os

@contextmanager
def Conexao2():
    load_dotenv('/home/grupompl/ModuloPCP/db.env')
    caminhoAbsoluto = os.getenv('CAMINHO')  # Troque por 'API_KEY' ou outro nome se necessário
    print(f'caminho: {caminhoAbsoluto}')
    conn = None
    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            'jdbc:Cache://10.162.0.193:1972/CONSISTEM',
            {'user': 'root', 'password': 'ccscache'},
            f'{caminhoAbsoluto}/connection/CacheDB.jar'
        )
        yield conn
    finally:
        if conn:
            conn.close()
@contextmanager
def ConexaoInternoMPL():
    conn = None
    load_dotenv('db.env')
    caminhoAbsoluto = os.getenv('CAMINHO')  # Troque por 'API_KEY' ou outro nome se necessário

    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            'jdbc:Cache://10.162.0.193:1972/CONSISTEM',
            {'user': '_system', 'password': 'ccscache'},
            f'{caminhoAbsoluto}/connection/CacheDB.jar'
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()