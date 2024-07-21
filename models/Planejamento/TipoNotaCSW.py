import gc
import pandas as pd
from connection import ConexaoBanco,ConexaoPostgreWms


def ObtendoTipoNotaCsw():
    sql = """SELECT f.codigo , f.descricao  FROM fat.TipoDeNotaPadrao f"""

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
