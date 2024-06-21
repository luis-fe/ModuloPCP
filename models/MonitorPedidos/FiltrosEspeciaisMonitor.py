import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco




def FiltroTipoNotas():
    SQL = """
    select TNM."cod_desTipoNota" as descricao  from "PCP".pcp."tipoNotaMonitor" tnm 
    """
    conn = ConexaoPostgreWms.conexaoEngine()
    consulta = pd.read_sql(SQL,conn)

    return consulta

def FiltroConceitoClientes():

    sql = """
    SELECT c.descricao  FROM fat.ConceitoCred c
    WHERE c.codEmpresa =1 
    """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sql)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            consulta = pd.DataFrame(rows, columns=colunas)
            del rows, colunas

    return consulta
