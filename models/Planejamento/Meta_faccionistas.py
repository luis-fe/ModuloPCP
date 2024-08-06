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
    consulta1_ = pd.merge(consulta1,consulta2,on='categoria',how='left')
    consulta1_['Capacidade/dia'].fillna(0,inplace=True)
    consulta1_.fillna('-',inplace=True)

    #Passo4 obtendo o exedente por categoria
    consulta1_['capacidadeSoma'] = consulta1_.groupby('categoria')['Capacidade/dia'].transform('sum')
    consulta1_['exedente'] = consulta1_['Meta Dia'] - consulta1_['capacidadeSoma']
    consulta1_ = consulta1_[consulta1_['exedente']>0]
    consulta1_ = consulta1_.groupby('categoria').agg({'exedente':'first'}).reset_index()

    consulta1_.rename(
        columns={'exedente': '01- AcordadoDia'},
        inplace=True)

    #Passo5 obtendo faccionistas
    Consultafaccionistas = RegistroFaccionistas2()

    resumo = pd.concat([Consultafaccionistas,consulta1_])
    resumo['nome'].fillna('EXCEDENTE',inplace=True)
    resumo.fillna('-',inplace=True)
    resumo['04-%Capacidade'] = resumo.groupby('categoria')['01- AcordadoDia'].transform('sum')
    resumo['04-%Capacidade'] = round(resumo['01- AcordadoDia']/resumo['04-%Capacidade']*100)
    resumo = resumo.sort_values(by=['categoria','01- AcordadoDia'], ascending=[True,False])
    resumo = pd.merge(resumo,consulta1,on='categoria')
    colunas_necessarias = ['01- AcordadoDi', '04-%Capacidade', 'categoria', 'codfaccionista', 'nome', 'FaltaProgramar',
                           'Fila']
    colunas_existentes = [col for col in colunas_necessarias if col in resumo.columns]
    resumo = resumo.loc[:, colunas_existentes]
    resumo['FaltaProgramar'] = resumo['FaltaProgramar'] * resumo['04-%Capacidade']
    resumo['Fila'] = resumo['Fila'] * resumo['04-%Capacidade']

    return resumo



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
