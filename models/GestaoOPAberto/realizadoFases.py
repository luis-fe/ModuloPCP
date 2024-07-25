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

    verifica = ComparativoMovimentacoes(100000)
    sql['chave'] = sql['numeroop']+'||'+sql['codfase'].astype(str)
    sql = pd.merge(sql,verifica,on='chave',how='left')
    sql['status'].fillna('-',inplace=True)
    sql = sql[sql['status'] == '-'].reset_index()
    sql = sql.drop(columns=['status','index'])
    print(sql)
    if sql['numeroop'].size > 0:
        #Implantando no banco de dados do Pcp
        ConexaoPostgreWms.Funcao_InserirOFF(sql, sql['numeroop'].size, 'realizado_fase', 'append')
    else:
        print('segue o baile')



def RealizadoMediaMovel(dataMovFaseIni,dataMovFaseFim ):
    sql = """
    select
	rf.numeroop ,
	rf.codfase:: varchar as "codFase", rf."seqRoteiro" , rf."dataBaixa"::date , rf."nomeFaccionista", rf."codFaccionista" , rf."horaMov"::time,
	rf."totPecasOPBaixadas" as "Realizado", rf."descOperMov" as operador, rf.chave 
from
	"PCP".pcp.realizado_fase rf 
where 
	rf."dataBaixa"::date >= %s 
	and rf."dataBaixa"::date <= %s ;
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    realizado = pd.read_sql(sql,conn,params=(dataMovFaseIni,dataMovFaseFim,))
    realizado = realizado.groupby(["codFase"]).agg({"Realizado":"sum"}).reset_index()

    return realizado




def ComparativoMovimentacoes(limit):

    sqlDelete = """
    delete from "PCP".pcp.realizado_fase 
    where "dataBaixa"::Date >=  CURRENT_DATE - INTERVAL '15 days'; 
    """

    conn1  =ConexaoPostgreWms.conexaoInsercao()
    curr = conn1.cursor()
    curr.execute(sqlDelete,)
    conn1.commit()
    curr.close()
    conn1.close()


    sql = """
    select distinct CHAVE, 'ok' as status from "PCP".pcp.realizado_fase
    order by CHAVE desc limit %s
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    consulta = pd.read_sql(sql,conn,params=(limit,))

    return consulta