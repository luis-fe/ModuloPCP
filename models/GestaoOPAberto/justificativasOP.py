'''
Arquivo para as justificativas no modulo Painel de Gestao da OP
'''
import gc

import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


'''Função de Consultar a Justificativa'''
def ConsultarJustificativa(ordemProd, fase):
    conn = ConexaoPostgreWms.conexaoEngine()
    consultaPostgre = 'SELECT ordemprod as "numeroOP", fase as "codFase", justificativa FROM "PCP".pcp.justificativa ' \
                'WHERE ordemprod = %s and fase = %s '
    consultaPostgre = pd.read_sql(consultaPostgre,conn,params=(ordemProd, fase,))



    sql = """SELECT CONVERT(varchar(12), codop) as numeroOP, codfase as codFase, textolinha as justificativa
    FROM tco.ObservacoesGiroFasesTexto  t 
    WHERE empresa = 1 and textolinha is not null
    """

    with ConexaoBanco.Conexao2() as connCsw:
        with connCsw.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            consultaCSW_justificativa = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()
    consultaCSW_justificativa['codFase'] = consultaCSW_justificativa['codFase'].astype(str)
    consultaCSW_justificativa = consultaCSW_justificativa[consultaCSW_justificativa['numeroOP'] == ordemProd]
    consultaCSW_justificativa = consultaCSW_justificativa[consultaCSW_justificativa['codFase'] == str(fase)]


    # Caso nao tenha justificativa no CSW porém tenha justificativa no banco do PCP
    if consultaCSW_justificativa.empty and not consultaPostgre.empty:
        consulta = consultaPostgre
        return consulta # Retorna Justificativa do Banco do PCP


    # Caso nao tenha justiicativa em ambos os bancos
    elif consultaCSW_justificativa.empty and consultaPostgre.empty:
        consulta = pd.DataFrame([{'justificativa': 'sem justificativa'}])
        return consulta

    # Caso  tenha justificativa no CSW porém nao tenha justificativa no banco do PCP
    elif not consultaCSW_justificativa.empty and consultaPostgre.empty:
        consulta = consultaCSW_justificativa
        return consulta

    # Caso tenha justificativa em ambos os bancos , assume do banco do postgre
    else:
        consulta = consultaPostgre
        return consulta


'''FUNCAO CADASTRAR A JUSTIFICATIVA '''
def CadastrarJustificativa(ordemProd, fase , justificativa):
    fase = str(fase)
    consultar = ConsultarJustificativa(ordemProd, fase)

    if consultar['justificativa'][0] == 'sem justificativa':
        conn = ConexaoPostgreWms.conexaoInsercao()

        insert = 'INSERT INTO "PCP".pcp.justificativa (ordemprod, fase, justificativa) values ' \
                 '(%s , %s, %s )'

        cursor = conn.cursor()
        cursor.execute (insert,(ordemProd, fase, justificativa))
        conn.commit()
        cursor.close()

        conn.close()
        return pd.DataFrame([{'mensagem':'Dados Inseridos com sucesso !'}])

    else:
        conn = ConexaoPostgreWms.conexaoInsercao()

        update = 'update "PCP".pcp.justificativa set justificativa = %s where ' \
                 ' ordemprod = %s and fase = %s '

        cursor = conn.cursor()
        cursor.execute (update,( justificativa, ordemProd, fase))
        conn.commit()
        cursor.close()

        conn.close()

        return pd.DataFrame([{'mensagem':'Dados Inseridos com sucesso !!'}])

