import gc
from models.GestaoOPAberto import realizadoFases
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
from models.Faccionistas import faccionistas

def MetasFaccionistas(codigoPlano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado):
    conn = ConexaoPostgreWms.conexaoEngine()

    # Carregando plano e capacidades em uma única consulta, se possível
    codLote = arrayCodLoteCsw[0]
    sql_plano = """
    SELECT mc.*, fc."Capacidade/dia"::int, fc.codfaccionista 
    FROM "backup"."metaCategoria" mc
    LEFT JOIN "PCP".pcp."faccaoCategoria" fc ON mc.categoria = fc.nomecategoria
    WHERE mc."plano" = %s AND mc."codLote" = %s
    """
    consulta = pd.read_sql(sql_plano, conn, params=(codigoPlano, codLote))
    consulta['Capacidade/dia'].fillna(0, inplace=True)

    # Calculando capacidade e exedente em uma única etapa
    consulta['capacidadeSoma'] = consulta.groupby('categoria')['Capacidade/dia'].transform('sum')
    consulta['exedente'] = consulta['Meta Dia'] - consulta['capacidadeSoma']
    consulta = consulta[consulta['exedente'] > 0].groupby('categoria').agg({'exedente': 'first'}).reset_index()

    consulta.rename(columns={'exedente': '01- AcordadoDia'}, inplace=True)

    # Merge com faccionistas e calculando %Capacidade
    Consultafaccionistas = RegistroFaccionistas2()
    resumo = pd.concat([Consultafaccionistas, consulta], ignore_index=True)
    resumo['nome'].fillna('EXCEDENTE', inplace=True)

    resumo['04-%Capacidade'] = resumo.groupby('categoria')['01- AcordadoDia'].transform('sum')
    resumo['04-%Capacidade'] = round(resumo['01- AcordadoDia'] / resumo['04-%Capacidade'] * 100)
    resumo = resumo.sort_values(by=['categoria', '01- AcordadoDia'], ascending=[True, False])

    resumo = pd.merge(resumo, consulta, on='categoria')
    print(resumo)

    resumo['FaltaProgramar'] = resumo['FaltaProgramar'] * (resumo['04-%Capacidade'] / 100)
    resumo['Fila'] = resumo['Fila'] * (resumo['04-%Capacidade'] / 100)

    # Calculando 'Falta Produzir' e 'Meta Dia' uma vez
    cargaFac = CargaFaccionista()
    resumo = pd.merge(resumo, cargaFac, on=['categoria', 'codfaccionista'], how='left')
    resumo['carga'].fillna(0, inplace=True)
    resumo['Falta Produzir'] = resumo[['carga', 'Fila', 'FaltaProgramar']].sum(axis=1)
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

    # Incorporando realizações
    Realizacao = realizadoFases.RemetidoFaseCategoriaFaccionista(dataMovFaseIni, dataMovFaseFim)
    resumo = pd.merge(resumo, Realizacao, on=['03- categoria', '00- codFac'], how='left')
    resumo['Remetido'].fillna(0, inplace=True)

    Retornado = realizadoFases.RetornadoFaseCategoriaFaccionista(dataMovFaseIni, dataMovFaseFim)
    resumo = pd.merge(resumo, Retornado, on=['03- categoria', '00- codFac'], how='left')
    resumo['Realizado'].fillna(0, inplace=True)

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

