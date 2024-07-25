import gc
from connection import ConexaoPostgreWms,ConexaoBanco
import pandas as pd



def CarregarRealizado(utimosDias):

    sql = """SELECT f.numeroop as numeroop, f.codfase as codfase, f.seqroteiro, f.databaixa, 
    f.nomeFaccionista, f.codFaccionista,
    f.horaMov, f.totPecasOPBaixadas, 
    f.descOperMov  FROM tco.MovimentacaoOPFase f
    WHERE f.codEmpresa = 1 and f.databaixa >=  DATEADD(DAY, -"""+str(utimosDias)+""", GETDATE())"""


    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            sql = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()

    sql['chave'] = sql['numeroop']+'||'+sql['codfase'].astype(str)

    if sql['numeroop'].size > 0:
        #Implantando no banco de dados do Pcp
        ConexaoPostgreWms.Funcao_InserirOFF(sql, sql['numeroop'].size, 'realizado_fase', 'replace')
    else:
        print('segue o baile')



def RealizadoMediaMovel():
    sql = """"""


