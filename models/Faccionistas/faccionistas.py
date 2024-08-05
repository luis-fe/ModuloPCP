import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


def ListaFaccionistasCsw():
    sql = """SELECT
	f.codFaccionista ,
	f.nome as nomeFaccionista
FROM
	tcg.Faccionista f
WHERE
	f.Empresa = 1"""
    conn = ConexaoBanco.ConexaoInternoMPL()
    consulta = pd.read_sql(sql,conn)
    conn.close()

    return consulta


def CadastrarCapacidadeDiariaFac(codFaccionista,apelido,ArrayCategorias, ArrayCapacidade):
    sql =""""""

def ObterCategorias():
    sql = """Select "nomecategoria" as categoria from pcp.categoria """
    conn = ConexaoPostgreWms.conexaoEngine()
    consulta = pd.read_sql(sql,conn)

    return consulta