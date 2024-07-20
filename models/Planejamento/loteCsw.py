import connection.ConexaoBanco as ConexaoBanco
import pandas as pd
import gc

from connection import ConexaoPostgreWms


def lote(empresa):
    if empresa == '1':
        sql = """ select codLote , descricao as nomeLote FROM tcl.Lote  l
            WHERE l.descricao like '%PREV%' and l.codEmpresa = 1 order by codLote desc """
    else:
        sql = """ select codLote , descricao as nomeLote FROM tcl.Lote  l
                    WHERE l.descricao like '%PREV%' and l.codEmpresa = 4 order by codLote desc """

    with ConexaoBanco.Conexao2() as conn: 
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            lotes = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()

    return lotes


def ExplodindoAsReferenciasLote(empresa, arrayCodLoteCsw):
    novo = ", ".join(arrayCodLoteCsw)

    sqlLotes = """
    select Empresa , t.codLote, codengenharia, t.codSeqTamanho , t.codSortimento , t.qtdePecasImplementadas as previsao FROM tcl.LoteSeqTamanho t
    WHERE t.Empresa = """+ empresa +"""and t.codLote in ("""+novo+""") 
    """

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sqlLotes)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            lotes = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()

    #Implantando no banco de dados do Pcp
    ConexaoPostgreWms.Funcao_InserirOFF(lotes, lotes['codengenharia'].size, 'lote_itens', 'replace')


    return lotes


