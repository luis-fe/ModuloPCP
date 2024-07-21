import connection.ConexaoBanco as ConexaoBanco
import pandas as pd
import gc

from connection import ConexaoPostgreWms
from models.Planejamento import itemsPA_Csw


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
    nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
    novo = ", ".join(nomes_com_aspas)
    print(novo)
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
    ConexaoPostgreWms.Funcao_InserirOFF(lotes, lotes['codLote'].size, 'lote_itens', 'append')
    itemsPA_Csw.RecarregarItens()
    CarregarRoteiroEngLote(empresa,arrayCodLoteCsw)

    return lotes


def DesvincularLotePlano(empresa, lote):
    # Passo 1: Excluir o lote do plano vinculado
    deletarLote = """DELETE FROM pcp."LoteporPlano" WHERE lote = %s """
    conn = ConexaoPostgreWms.conexaoInsercao()
    cur = conn.cursor()
    cur.execute(deletarLote, (lote,))
    conn.commit()


    # Passo 2: Verifica se o lote existe em outros planos
    conn2 = ConexaoPostgreWms.conexaoEngine()
    sql = """Select lote from pcp."LoteporPlano" WHERE lote = %s """
    verifca = pd.read_sql(sql,conn2, params=(lote,))

    if verifca.empty:

        deletarLoteIntens = """Delete from pcp.lote_itens where "codLote" = %s  """
        cur.execute(deletarLoteIntens, (lote,))
        conn.commit()

    else:
        print('sem lote para exlcuir dos lotes engenharias')
    cur.close()
    conn.close()

def ConsultarLoteEspecificoCsw(empresa, codLote):
    sql = """Select codLote, descricao as nomeLote from tcl.lote where codEmpresa= """+str(empresa)+""" and codLote ="""+ "'"+codLote+"'"

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            lotes = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()

    return lotes['nomeLote'][0]

def CarregarRoteiroEngLote(empresa, arrayCodLoteCsw):
    nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
    novo = ", ".join(nomes_com_aspas)

    sql = """
    SELECT p.codEngenharia , p.codFase , p.nomeFase, p.seqProcesso  FROM tcp.ProcessosEngenharia p
WHERE p.codEmpresa = 1 and p.codEngenharia like '%-0' and 
p.codEngenharia in (select l.codEngenharia from tcl.LoteEngenharia l WHERE l.empresa ="""+str(empresa) + """ 
and l.codlote in ( """+ novo+"""))"""

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            EngRoteiro = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()

    # Verificando as engenharias que ja existe:
    sqlPCP = """select distinct "codEngenharia", 'ok' as situacao from pcp."Eng_Roteiro" """

    conn2 = ConexaoPostgreWms.conexaoEngine()
    sqlPCP = pd.read_sql(sqlPCP,conn2)

    EngRoteiro = pd.merge(EngRoteiro,sqlPCP,on='codEngenharia',how='left')
    EngRoteiro.fillna('-',inplace=True)
    EngRoteiro = EngRoteiro[EngRoteiro['situacao']=='-'].reset_index()

    try:
        #Implantando no banco de dados do Pcp
        ConexaoPostgreWms.Funcao_InserirOFF(EngRoteiro, EngRoteiro['codEngenharia'].size, 'Eng_Roteiro', 'append')
    except:
        print('segue o baile')

