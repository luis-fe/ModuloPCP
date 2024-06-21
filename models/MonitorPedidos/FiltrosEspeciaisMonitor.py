import pandas as pd
from connection import ConexaoPostgreWms




def FiltroTipoNotas():
    SQL = """
    select TNM."cod_desTipoNota"  from "PCP".pcp."tipoNotaMonitor" tnm 
    """
    conn = ConexaoPostgreWms.conexaoEngine()
    consulta = pd.read_sql(SQL,conn)

    return consulta
