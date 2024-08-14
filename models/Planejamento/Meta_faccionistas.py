import gc
from models.GestaoOPAberto import realizadoFases
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
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
    consulta1_ = consulta1_[consulta1_['exedente'] > 0].groupby('categoria').agg({'exedente': 'first'}).reset_index()


    consulta1_.rename(
        columns={'exedente': '01- AcordadoDia'},
        inplace=True)

    #Passo5 obtendo faccionistas e agregando o exedente ao registro de faccionista
    Consultafaccionistas = RegistroFaccionistas2()

    resumo = pd.concat([Consultafaccionistas,consulta1_], ignore_index=True)
    resumo['nome'].fillna('EXCEDENTE',inplace=True)
    resumo.fillna('-',inplace=True)
    resumo['04-%Capacidade'] = resumo.groupby('categoria')['01- AcordadoDia'].transform('sum')
    resumo['04-%Capacidade'] = round(resumo['01- AcordadoDia']/resumo['04-%Capacidade']*100)
    resumo = resumo.sort_values(by=['categoria','01- AcordadoDia'], ascending=[True,False])


    resumo = pd.merge(resumo,consulta1,on='categoria')

    colunas_necessarias = ['01- AcordadoDia', '04-%Capacidade', 'categoria', 'codfaccionista', 'nome', 'FaltaProgramar',
                           'Fila','dias']
    colunas_existentes = [col for col in colunas_necessarias if col in resumo.columns]
    resumo = resumo.loc[:, colunas_existentes]
    resumo['FaltaProgramar'] = resumo['FaltaProgramar'] * (resumo['04-%Capacidade']/100)
    resumo['Fila'] = resumo['Fila'] * (resumo['04-%Capacidade']/100)
    resumo['Fila'] = resumo['Fila'].round(0)
    resumo['FaltaProgramar'] = resumo['FaltaProgramar'].round(0)
    cargaFac = CargaFaccionista()
    resumo = pd.merge(resumo,cargaFac,on=['categoria','codfaccionista'],how='left')
    resumo['carga'].fillna(0,inplace=True)
    resumo['Falta Produzir'] = resumo['carga'] + resumo['Fila'] + resumo['FaltaProgramar']
    resumo['Meta Dia'] = (resumo['Falta Produzir'] / resumo['dias']).round(0)

    # Renomeando colunas
    resumo.rename(columns={
        'codfaccionista': '00- codFac',
        'nome': '01-nomeFac',
        'categoria': '03- categoria',
        '01- AcordadoDia': '04- AcordadoDia',
        '04-%Capacidade': '05-%Cap.',
        'FaltaProgramar': '06-FaltaProgramar',
        'Fila': '07-Fila',
        'carga': '08-Carga',
        'Falta Produzir': '09-Falta Produzir',
        'dias': '10-dias',
        'Meta Dia': '11-Meta Dia'}, inplace=True)

    Realizacao = realizadoFases.RemetidoFaseCategoriaFaccionista(dataMovFaseIni, dataMovFaseFim)
    resumo = pd.merge(resumo,Realizacao,on=['03- categoria','00- codFac'],how='left')
    resumo['Remetido'].fillna(0,inplace=True)

    Retornado = realizadoFases.RetornadoFaseCategoriaFaccionista(dataMovFaseIni, dataMovFaseFim)
    resumo = pd.merge(resumo,Retornado,on=['03- categoria','00- codFac'],how='left')
    resumo['Realizado'].fillna(0,inplace=True)

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


def CargaFaccionista():
    sql = """
    SELECT op.numeroOP, op.codProduto, d.codOP, d.codFac as codfaccionista,l.qtdPecasRem as carga, e.descricao as nome  
FROM tco.OrdemProd op 
left join tct.RemessaOPsDistribuicao d on d.Empresa = 1 and d.codOP = op.numeroOP and d.situac = 2 and d.codFase = op.codFaseAtual 
left join tct.RemessasLoteOP l on l.Empresa = d.Empresa  and l.codRemessa = d.numRem 
join tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
WHERE op.codEmpresa =1 and op.situacao =3 and op.codFaseAtual in (455, 459, 429)
    """
    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            consulta = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()
    consulta['categoria'] = '-'
    consulta['categoria'] = consulta['nome'].apply(mapear_categoria)

    consulta = consulta.groupby(['categoria','codfaccionista']).agg({'carga':'sum'}).reset_index()
    consulta['codfaccionista'] =consulta['codfaccionista'].astype(str)
    return consulta





def mapear_categoria(nome):
    categorias_map = {
        'CAMISA': 'CAMISA',
        'POLO': 'POLO',
        'BATA': 'CAMISA',
        'TRICOT': 'TRICOT',
        'BONE': 'BONE',
        'CARTEIRA': 'CARTEIRA',
        'TSHIRT': 'CAMISETA',
        'REGATA': 'CAMISETA',
        'BLUSAO': 'AGASALHOS',
        'BABY': 'CAMISETA',
        'JAQUETA': 'JAQUETA',
        'CINTO': 'CINTO',
        'PORTA CAR': 'CARTEIRA',
        'CUECA': 'CUECA',
        'MEIA': 'MEIA',
        'SUNGA': 'SUNGA',
        'SHORT': 'SHORT',
        'BERMUDA': 'BERMUDA'
    }
    for chave, valor in categorias_map.items():
        if chave in nome.upper():
            return valor
    return '-'

