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

    verifica = ComparativoMovimentacoes(20000)
    sql['chave'] = sql['numeroop']+'||'+sql['codfase'].astype(str)
    sql = pd.merge(sql,verifica,on='chave',how='left')
    sql['status'].fillna('-',inplace=True)
    sql = sql[sql['status'] == '-'].reset_index()
    sql = sql.drop(columns=['status','index'])
    #print(sql)
    if sql['numeroop'].size > 0:
        #Implantando no banco de dados do Pcp
        ConexaoPostgreWms.Funcao_InserirOFF(sql, sql['numeroop'].size, 'realizado_fase', 'append')
    else:
        print('segue o baile')



def RealizadoMediaMovel(dataMovFaseIni,dataMovFaseFim):
    CarregarRealizado(60)

    sql = """
    select rf."codEngenharia",
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
    realizado['filtro'] = realizado['codFase'].astype(str) + '|'+realizado['codEngenharia'].str[0]
    realizado = realizado[(realizado['filtro']!='401|6')]
    realizado = realizado[(realizado['filtro']!='401|5')]
    realizado = realizado[(realizado['filtro']!='426|6')]
    realizado = realizado[(realizado['filtro']!='441|5')]
    realizado = realizado[(realizado['filtro']!='412|5')]

    realizado['codFase'] = np.where(realizado['codFase'].isin(['431', '455', '459']), '429', realizado['codFase'])

    realizado = realizado.groupby(["codFase"]).agg({"Realizado":"sum"}).reset_index()

    diasUteis = calcular_dias_sem_domingos(dataMovFaseIni,dataMovFaseFim)
    # Evitar divisão por zero ou infinito
    realizado['Realizado'] = np.where(diasUteis == 0, 0, realizado['Realizado'] / diasUteis)
    print(f'dias uteis {diasUteis}')
    return realizado

def RealizadoFaseCategoria(dataMovFaseIni,dataMovFaseFim,codFase):
    CarregarRealizado(60)

    sql = """
        select rf."codEngenharia",
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
    realizado = pd.read_sql(sql, conn, params=(dataMovFaseIni, dataMovFaseFim,))

    realizado['codFase'] = np.where(realizado['codFase'].isin(['431', '455', '459']), '429', realizado['codFase'])
    realizado = realizado[realizado['codFase']==str(codFase)].reset_index()

    conn = ConexaoPostgreWms.conexaoEngine()
    sqlNomeEngenharia = """
    select ic."codItemPai"::varchar , max(ic.nome)::varchar as nome from "PCP".pcp.itens_csw ic where ("codItemPai" like '1%') or ("codItemPai" like '5%') group by "codItemPai"
    """
    NomeEngenharia = pd.read_sql(sqlNomeEngenharia,conn)

    NomeEngenharia['codEngenharia'] = NomeEngenharia.apply(
        lambda r: '0' + r['codItemPai'] + '-0' if r['codItemPai'].startswith('1') else r['codItemPai'] + '-0', axis=1)
    realizado = pd.merge(realizado,NomeEngenharia,on='codEngenharia',how='left')
    realizado['categoria'] = '-'
    realizado['nome'] = realizado['nome'].astype(str)
    realizado['categoria'] = realizado['nome'].apply(mapear_categoria)
    realizado = realizado.groupby(["codFase","categoria"]).agg({"Realizado":"sum"}).reset_index()

    diasUteis = calcular_dias_sem_domingos(dataMovFaseIni,dataMovFaseFim)
    # Evitar divisão por zero ou infinito
    realizado['Realizado'] = np.where(diasUteis == 0, 0, realizado['Realizado'] / diasUteis)
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


def mapear_categoria(nome):
    categorias_map = {
        'CAMISA': 'CAMISA',
        'POLO': 'POLO',
        'BATA': 'CAMISA',
        'TRICOT': 'TRICOT',
        'BONE': 'BONE',
        'CARTEIRA': 'CARTEIRA',
        'TSHIRT': 'CAMISETA',
        'REGATA': 'CAMISETA',
        'BLUSAO': 'AGASALHOS',
        'BABY': 'CAMISETA',
        'JAQUETA': 'JAQUETA',
        'CINTO': 'CINTO',
        'PORTA CAR': 'CARTEIRA',
        'CUECA': 'CUECA',
        'MEIA': 'MEIA',
        'SUNGA': 'SUNGA',
        'SHORT': 'SHORT',
        'BERMUDA': 'BERMUDA'
    }
    for chave, valor in categorias_map.items():
        if chave in nome.upper():
            return valor
    return '-'



def RealizadoFaseCategoriaFaccionista(dataMovFaseIni,dataMovFaseFim,codFase):
    CarregarRealizado(60)

    sql = """
        select rf."codEngenharia",
    	rf.numeroop ,
    	rf.codfase:: varchar as "codFase", rf."seqRoteiro" , rf."dataBaixa"::date , rf."nomeFaccionista", rf."codFaccionista" as '00- codFac' , rf."horaMov"::time,
    	rf."totPecasOPBaixadas" as "Realizado", rf."descOperMov" as operador, rf.chave 
    from
    	"PCP".pcp.realizado_fase rf 
    where 
    	rf."dataBaixa"::date >= %s 
    	and rf."dataBaixa"::date <= %s ;
        """

    conn = ConexaoPostgreWms.conexaoEngine()
    realizado = pd.read_sql(sql, conn, params=(dataMovFaseIni, dataMovFaseFim,))

    realizado['codFase'] = np.where(realizado['codFase'].isin(['431', '455', '459']), '429', realizado['codFase'])
    realizado = realizado[realizado['codFase']==str(codFase)].reset_index()

    conn = ConexaoPostgreWms.conexaoEngine()
    sqlNomeEngenharia = """
    select ic."codItemPai"::varchar , max(ic.nome)::varchar as nome from "PCP".pcp.itens_csw ic where ("codItemPai" like '1%') or ("codItemPai" like '5%') group by "codItemPai"
    """
    NomeEngenharia = pd.read_sql(sqlNomeEngenharia,conn)

    NomeEngenharia['codEngenharia'] = NomeEngenharia.apply(
        lambda r: '0' + r['codItemPai'] + '-0' if r['codItemPai'].startswith('1') else r['codItemPai'] + '-0', axis=1)
    realizado = pd.merge(realizado,NomeEngenharia,on='codEngenharia',how='left')
    realizado['categoria'] = '-'
    realizado['nome'] = realizado['nome'].astype(str)
    realizado['categoria'] = realizado['nome'].apply(mapear_categoria)
    realizado = realizado.groupby(["codFase","categoria","00- codFac"]).agg({"Realizado":"sum"}).reset_index()

    diasUteis = calcular_dias_sem_domingos(dataMovFaseIni,dataMovFaseFim)
    # Evitar divisão por zero ou infinito
    realizado['Realizado'] = np.where(diasUteis == 0, 0, realizado['Realizado'] / diasUteis)
    realizado['00- codFac'] = realizado['00- codFac'] .astype(str)
    return realizado