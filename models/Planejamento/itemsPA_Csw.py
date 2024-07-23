import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


def RecarregarItens():
    # passo 1: obter ultimo registro do sql itens_csw
    sql = """
    select max(codigo::int) as maximo from "PCP".pcp.itens_csw
    """

    conn = ConexaoPostgreWms.conexaoEngine()

    sqlMax = pd.read_sql(sql,conn)
    maximo = sqlMax['maximo'][0]


    sqlCSWItens = """
SELECT i.codigo , i.nome , i.unidadeMedida, i2.codItemPai, i2.codSortimento , i2.codSeqTamanho  FROM cgi.Item i
JOIN Cgi.Item2 i2 on i2.coditem = i.codigo and i2.Empresa = 1
WHERE i.unidadeMedida in ('PC','KIT') and (i2.codItemPai like '1%' or i2.codItemPai like '2%'or i2.codItemPai like '3%'or i2.codItemPai like '5%'or i2.codItemPai like '6%' )
and i2.codItemPai > 0 and i.codigo > """+str(maximo)

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sqlCSWItens)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            itens = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
    del rows
    gc.collect()

    try:
        #Implantando no banco de dados do Pcp
        ConexaoPostgreWms.Funcao_InserirOFF(itens, itens['codigo'].size, 'itens_csw', 'append')
    except:
        print('segue o baile ')
    return itens


def EstoqueNaturezaPA():
    sql = """
    SELECT d.codItem , d.estoqueAtual  FROM est.DadosEstoque d
    WHERE d.codNatureza = 5 and d.codEmpresa = 1 and d.estoqueAtual > 0
    """

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            estoque = pd.DataFrame(rows, columns=colunas)

    return estoque


def CargaFases():

    sqlCsw = """
    SELECT op.numeroOP as numeroop ,op.codFaseAtual  FROM tco.OrdemProd op 
WHERE op.situacao = 3 and op.codEmpresa = 1 and op.codFaseAtual <> 401
    """

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sqlCsw)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            sqlCsw = pd.DataFrame(rows, columns=colunas)

    sql = """
    select codreduzido as "codItem", total_pcs as carga, o.numeroop  from pcp.ordemprod o 
    where codreduzido is not null
    """

    conn2 = ConexaoPostgreWms.conexaoEngine()
    cargas = pd.read_sql(sql,conn2)

    cargas = pd.merge(cargas,sqlCsw,on='numeroop')

    cargas = cargas.groupby(["codItem"]).agg({"carga":"sum"}).reset_index()


    return cargas



def EstoquePartes():

    sql = """
    SELECT cv.CodComponente as redParte, cv.codProduto, cv2.codSortimento, cv2.seqTamanho as codSeqTamanho, cv2.quantidade
    FROM tcp.ComponentesVariaveis cv
    inner join tcp.CompVarSorGraTam cv2 on cv2.codEmpresa = cv.codEmpresa and cv2.codProduto = cv.codProduto and cv.codSequencia = cv2.sequencia 
    WHERE cv.codEmpresa = 1 and cv.codClassifComponente in (10,12) and cv.codProduto like '%-0'
    """

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            relacaoPartes = pd.DataFrame(rows, columns=colunas)

    relacaoPartes['codSeqTamanho'] = relacaoPartes['codSeqTamanho'].astype(str)
    relacaoPartes['codSortimento'] = relacaoPartes['codSortimento'].astype(str)

    sl2Itens2 = """
    select codigo as codItem, "codSortimento", "codSeqTamanho", '0'||"codItemPai"||'-0' as "codProduto"  from "PCP".pcp.itens_csw ic 
    where ic."codItemPai" like '1%'
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    itens = pd.read_sql(sl2Itens2,conn)

    relacaoPartes = pd.merge(relacaoPartes,itens,on=['codProduto','codSortimento','codSeqTamanho'])


    estoquePa = EstoqueNaturezaPA()
    relacaoPartes = pd.merge(relacaoPartes,estoquePa,on='codItem')


    relacaoPartes['estoqueAtual'] = relacaoPartes['quantidade'] * relacaoPartes['estoqueAtual']
    relacaoPartes.drop(['codItem','quantidade','codProduto','codSortimento','codSeqTamanho'], axis=1, inplace=True)
    relacaoPartes.rename(columns={'redParte': 'codItem'}, inplace=True)

    relacaoPartes = pd.concat([estoquePa,relacaoPartes])

    return relacaoPartes