import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco

def Fases():

    sql = """ SELECT f.codFase as codFase , f.nome as nomeFase  FROM tcp.FasesProducao f
            WHERE f.codEmpresa = 1 and f.codFase > 400 and f.codFase < 500 """

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            fases = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()
    fases['codFase'] = fases['codFase'].astype(str)
    return fases