import pandas as pd
from connection import ConexaoPostgreWms
from models.Faccionistas import faccionistas

def MetasFaccionistas(codigoPlano,arrayCodLoteCsw,dataMovFaseIni, dataMovFaseFim, congelado):
    # passo 1 Carregar o plano informado
    conn = ConexaoPostgreWms.conexaoEngine()
    sql = """select * from "backup"."metaCategoria" where "plano" = %s and "codLote" = %s """

    codLote = arrayCodLoteCsw[0]
    consulta1 = pd.read_sql(sql,conn,params=(codigoPlano,codLote,))

    #Passo2 carregando as capacidades dos faccionistas
    sql = """select nomecategoria as categoria, codfaccionista, "Capacidade/dia"::int from "PCP".pcp."faccaoCategoria" fc   """
    consulta2 = pd.read_sql(sql,conn)

    #Passo3 Realizando o merge
    consulta1 = pd.merge(consulta1,consulta2,on='categoria',how='left')
    consulta1['Capacidade/dia'].fillna(0,inplace=True)
    consulta1.fillna('-',inplace=True)

    #Passo4 obtendo o exedente por categoria
    consulta1['capacidadeSoma'] = consulta1.groupby('categoria')['Capacidade/dia'].transform('sum')
    consulta1['exedente'] = consulta1['Meta Dia'] - consulta1['capacidadeSoma']
    consulta1_ = consulta1[consulta1['exedente']>0]
    consulta1_ = consulta1_.groupby('categoria').agg({'exedente':'first'}).reset_index()

    #Passo5 obtendo faccionistas
    Consultafaccionistas = RegistroFaccionistas2()



    return Consultafaccionistas



def RegistroFaccionistas2():
    sql = """SELECT * FROM pcp.faccionista """
    sql2 = """SELECT * FROM pcp."faccaoCategoria" """

    conn = ConexaoPostgreWms.conexaoEngine()
    sql = pd.read_sql(sql,conn)
    sql2 = pd.read_sql(sql2,conn)
    merged = pd.merge(sql, sql2, on='codfaccionista', how='left')
    merged.fillna('-',inplace=True)
    merged['nome'] = merged.apply(lambda r: r['apelidofaccionista'] if r['apelidofaccionista'] != '-' else r['nomefaccionista'],axis=1)
    merged = merged.loc[:, ['Capacidade/dia', 'codfaccionista', 'nome', 'nomecategoria']]
    merged['Capacidade/dia'] = merged['Capacidade/dia'].astype(int)
    merged.rename(
        columns={'Capacidade/dia': '01- AcordadoDia',  'nomecategoria': 'categoria'},
        inplace=True)
    return merged
