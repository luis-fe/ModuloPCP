import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


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
    select codigo as "codItem", "codSortimento"::varchar, "codSeqTamanho"::varchar, '0'||"codItemPai"||'-0' as "codProduto"  from "PCP".pcp.itens_csw ic 
    where ic."codItemPai" like '1%'
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    itens = pd.read_sql(sl2Itens2,conn)

    relacaoPartes = pd.merge(relacaoPartes,itens,on=['codProduto','codSortimento','codSeqTamanho'])


    estoquePa = EstoqueNaturezaPA()
    relacaoPartes = pd.merge(relacaoPartes,estoquePa,on='codItem')

    cargaFase = CargaFases()
    cargaFasePartes = pd.merge(relacaoPartes, cargaFase, on='codItem')

    relacaoPartes['estoqueAtual'] = relacaoPartes['quantidade'] * relacaoPartes['estoqueAtual']
    relacaoPartes.drop(['codItem','codProduto','codSortimento','codSeqTamanho'], axis=1, inplace=True)
    cargaFasePartes.drop(['codItem','codProduto','codSortimento','codSeqTamanho','estoqueAtual'], axis=1, inplace=True)

    relacaoPartes.rename(columns={'redParte': 'codItem'}, inplace=True)
    cargaFasePartes.rename(columns={'redParte': 'codItem'}, inplace=True)


    relacaoPartes = pd.concat([estoquePa,relacaoPartes])

    cargaFasePartes = pd.concat([cargaFase,cargaFasePartes])
    cargaFasePartes.fillna(1,inplace=True)
    cargaFasePartes = cargaFasePartes.drop_duplicates()
    cargaFasePartes = cargaFasePartes.groupby('codItem').agg({'quantidade':'first','carga':'first'}).reset_index()
    relacaoPartes = relacaoPartes.groupby('codItem').agg({'quantidade':'first','estoqueAtual':'first'}).reset_index()


    return relacaoPartes
