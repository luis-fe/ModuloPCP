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