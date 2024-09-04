import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


def ListaFaccionistasCsw():
    sql = """SELECT
	f.codFaccionista ,
	f.nome as nomeFaccionista
FROM
	tcg.Faccionista f
WHERE
	f.Empresa = 1 order by nome """
    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            consulta = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()

    return consulta





def ObterCategorias():
    sql = """Select "nomecategoria" as categoria from pcp.categoria """
    conn = ConexaoPostgreWms.conexaoEngine()
    consulta = pd.read_sql(sql,conn)

    return consulta
