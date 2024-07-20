import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


def RecarregarItens():
    sqlCSWItens = """
    SELECT i.codigo , i.nome , i.unidadeMedida, i2.codItemPai, i2.codSortimento , i2.codSeqTamanho  FROM cgi.Item i
    JOIN Cgi.Item2 i2 on i2.coditem = i.codigo and i2.Empresa = 1
    WHERE i.unidadeMedida in ('PC','KIT')
    """

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sqlCSWItens)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            itens = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
    del rows
    gc.collect()


    #Implantando no banco de dados do Pcp
    ConexaoPostgreWms.Funcao_InserirOFF(itens, itens['codigo'].size, 'itens_csw', 'replace')

    return itens