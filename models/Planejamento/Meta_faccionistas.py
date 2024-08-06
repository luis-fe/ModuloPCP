import pandas as pd
from connection import ConexaoPostgreWms


def MetasFaccionistas(codigoPlano,arrayCodLoteCsw,dataMovFaseIni, dataMovFaseFim, congelado):
    # passo 1 Carregar o plano informado
    conn = ConexaoPostgreWms.conexaoEngine()
    sql = """select * from "backup"."metaCategoria" where "plano" = %s and "codLote" = %s """

    codLote = arrayCodLoteCsw[0]
    consulta1 = pd.read_sql(sql,conn,params=(codigoPlano,codLote,))

    #Passo2 carregando as capacidades dos faccionistas
    sql = """select nomecategoria as categoria, codfaccionista, "Capacidade/dia"::int from "PCP".pcp."faccaoCategoria" fc  """
    consulta2 = pd.read_sql(sql,conn)

    #Passo3 Realizando o merge
    consulta1 = pd.merge(consulta1,consulta2,on='categoria',how='left')
    consulta1['Capacidade/dia'].fillna(0,inplace=True)
    consulta1.fillna('-',inplace=True)

    consulta1['capacidadeSoma'] = consulta1.groupby('categoria')['Capacidade/dia'].transform('sum')
    consulta1['exedente'] = consulta1['Meta Dia'] - consulta1['capacidadeSoma']
    consulta1_ = consulta1[consulta1['exedente']>0]
    consulta1_ = consulta1_.groupby('categoria').agg({'exedente':'first'}).reset_index()


    return consulta1_