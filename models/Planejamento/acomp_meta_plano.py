import pandas as pd
from connection import ConexaoPostgreWms,ConexaoBanco


def MetasFase(plano, arrayCodLoteCsw):
    nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
    novo = ", ".join(nomes_com_aspas)

    sqlMetas = """select "codLote", 
    "Empresa" , "codEngenharia" , "codSeqTamanho" , "codSortimento" , previsao  
    from "PCP".pcp.lote_itens li where  "codLote" in ("""+novo+""")"""

    sqlRoteiro = """
    select * from "PCP".pcp."Eng_Roteiro" er 
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    sqlMetas = pd.read_sql(sqlMetas,conn)
    sqlRoteiro = pd.read_sql(sqlRoteiro,conn)

    Meta = sqlMetas.groupby(["codEngenharia" , "codSeqTamanho" , "codSortimento"]).agg({"previsao":"sum"}).reset_index()
    Meta = pd.merge(Meta,sqlRoteiro,on='codEngenharia',how='left')
    Meta = Meta.groupby(["codFase" , "nomeFase"]).agg({"previsao":"sum"}).reset_index()

    return Meta

