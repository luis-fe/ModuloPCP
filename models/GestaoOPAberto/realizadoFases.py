import gc
from connection import ConexaoPostgreWms,ConexaoBanco
import pandas as pd
import numpy as np
import pytz
from datetime import datetime


def obterdiaAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%Y-%m-%d')
    return pd.to_datetime(agora)

def calcular_dias_sem_domingos(dataInicio, dataFim):
    # Obtendo a data atual
    dataHoje = obterdiaAtual()
    # Convertendo as datas para o tipo datetime, se necessário
    if not isinstance(dataInicio, pd.Timestamp):
        dataInicio = pd.to_datetime(dataInicio)
    if not isinstance(dataFim, pd.Timestamp):
        dataFim = pd.to_datetime(dataFim)
    if not isinstance(dataHoje, pd.Timestamp):
        dataHoje = pd.to_datetime(dataFim)

    # Inicializando o contador de dias
    dias = 0
    data_atual = dataInicio

    # Iterando através das datas
    while data_atual <= dataFim:
        # Se o dia não for sábado (5) ou domingo (6), incrementa o contador de dias
        if data_atual.weekday() != 5 and data_atual.weekday() != 6:
            dias += 1
        # Incrementa a data atual em um dia
        data_atual += pd.Timedelta(days=1)

    return dias
def CarregarRealizado(utimosDias):

    sql = """SELECT f.numeroop as numeroop, f.codfase as codfase, f.seqroteiro, f.databaixa, 
    f.nomeFaccionista, f.codFaccionista,
    f.horaMov, f.totPecasOPBaixadas, 
    f.descOperMov, (select op.codProduto  from tco.ordemprod op WHERE op.codempresa = 1 and op.numeroop = f.numeroop) as codEngenharia  FROM tco.MovimentacaoOPFase f
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

    verifica = ComparativoMovimentacoes(500000)
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



def RealizadoMediaMovel(dataMovFaseIni,dataMovFaseFim):
    CarregarRealizado(60)

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

    diasUteis = calcular_dias_sem_domingos(dataMovFaseIni,dataMovFaseFim)
    # Evitar divisão por zero ou infinito
    realizado['Realizado'] = np.where(diasUteis == 0, 0, realizado['Realizado'] / diasUteis)
    print(f'dias uteis {diasUteis}')
    return realizado




def ComparativoMovimentacoes(limit):

    sqlDelete = """
    delete from "PCP".pcp.realizado_fase 
    where "dataBaixa"::Date >=  CURRENT_DATE - INTERVAL '500 days'; 
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