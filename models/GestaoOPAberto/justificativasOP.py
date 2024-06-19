'''
Arquivo para as justificativas no modulo Painel de Gestao da OP
'''
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco

def ConsultarJustificativa(ordemProd, fase):
    conn = ConexaoPostgreWms.conexaoEngine()
    conn2 = ConexaoBanco.Conexao2()

    consultaPostgre = 'SELECT ordemprod as "numeroOP", fase as "codFase", justificativa FROM "PCP".pcp.justificativa ' \
                'WHERE ordemprod = %s and fase = %s '

    consultaPostgre = pd.read_sql(consultaPostgre,conn,params=(ordemProd, fase,))


    consulta2 =pd.read_sql('SELECT CONVERT(varchar(12), codop) as numeroOP, codfase as codFase, textolinha as justificativa FROM tco.ObservacoesGiroFasesTexto  t '
                                    'WHERE empresa = 1 and textolinha is not null',conn2)
    consulta2['codFase'] = consulta2['codFase'].astype(str)

    consulta2 = consulta2[consulta2['numeroOP'] == ordemProd]
    consulta2 = consulta2[consulta2['codFase'] == str(fase)]

    conn2.close()

    if consulta2.empty and not consultaPostgre.empty:
        consulta = consultaPostgre
        print('teste1')

    elif consulta2.empty and consultaPostgre.empty:
        consulta = pd.DataFrame([{'justificativa': 'sem justificativa'}])

    elif not consulta2.empty and consultaPostgre.empty:

        consulta = pd.DataFrame([{'justificativa': 'sem justificativa'}])


    else:
        consulta = consultaPostgre


    return consulta

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

