import connection.ConexaoBanco as ConexaoBanco
import pandas as pd
import gc

def lote(empresa):
    sql = """SELECT * FROM TCL.lote l WHERE l.codempresa = %s""" % empresa

    lotes = pd.DataFrame()  # Inicializa um DataFrame vazio para armazenar os dados

    with ConexaoBanco.Conexao2() as conn: 
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            lotes = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()

    return lotes
