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



def RemetidoFaseCategoriaFaccionista(dataMovFaseIni, dataMovFaseFim):

    sql = """
SELECT
	r.codFase ,
	r.codFaccio as codFaccionista ,
	r.codOP ,
	r.qtdRemetidas as Remetido ,
	r.dataEmissao, op.codProduto , e.descricao as nome
FROM
	tct.RetSimbolicoNF r
inner join 
	tco.OrdemProd op on op.codEmpresa = 1 and op.numeroOP = r.codOP 
inner JOIN 
	tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
WHERE
	r.Empresa = 1 and r.codFase in (429, 431, 455, 459) and r.dataEmissao >= '"""+dataMovFaseIni+"""'and r.dataEmissao <=  '"""+dataMovFaseFim +"""'"""
    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            realizado = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()


    realizado['categoria'] = '-'
    realizado['nome'] = realizado['nome'].astype(str)
    realizado['categoria'] = realizado['nome'].apply(mapear_categoria)

    realizado = realizado.groupby(["categoria","codFaccionista"]).agg({"Remetido":"sum"}).reset_index()

    diasUteis = calcular_dias_sem_domingos(dataMovFaseIni,dataMovFaseFim)
    # Evitar divisão por zero ou infinito
    realizado['Remetido'] = np.where(diasUteis == 0, 0, realizado['Remetido'] / diasUteis)
    realizado['codFaccionista'] = realizado['codFaccionista'] .astype(str)
    realizado.rename(
        columns={'categoria': '03- categoria','codFaccionista':'00- codFac'},
        inplace=True)

    return realizado

def RetornadoFaseCategoriaFaccionista(dataMovFaseIni, dataMovFaseFim):

    sql = """
SELECT
	r.codFase  ,
	r.codFaccio as codFaccionista,
	r.codOP ,
	r.quantidade as Realizado ,
	r.dataEntrada , op.codProduto , e.descricao as nome
FROM
	tct.RetSimbolicoNFERetorno r
inner join 
	tco.OrdemProd op on op.codEmpresa = 1 and op.numeroOP = r.codOP 
inner JOIN 
	tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
WHERE
	r.Empresa = 1 and r.codFase in (429, 431, 455, 459) and r.dataEntrada >= '"""+dataMovFaseIni+"""'and r.dataEntrada <=  '"""+dataMovFaseFim +"""'"""
    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            realizado = pd.DataFrame(rows, columns=colunas)

    # Libera memória manualmente
    del rows
    gc.collect()


    realizado['categoria'] = '-'
    realizado['nome'] = realizado['nome'].astype(str)
    realizado['categoria'] = realizado['nome'].apply(mapear_categoria)

    realizado = realizado.groupby(["categoria","codFaccionista"]).agg({"Realizado":"sum"}).reset_index()

    diasUteis = calcular_dias_sem_domingos(dataMovFaseIni,dataMovFaseFim)
    # Evitar divisão por zero ou infinito
    realizado['Realizado'] = np.where(diasUteis == 0, 0, realizado['Realizado'] / diasUteis)
    realizado['codFaccionista'] = realizado['codFaccionista'] .astype(str)
    realizado.rename(
        columns={'categoria': '03- categoria','codFaccionista':'00- codFac'},
        inplace=True)

    return realizado

def LeadTimeRealizado(dataMovFaseIni, dataMovFaseFim):

    sqlMovPCP = """
        select
    	rf.numeroop as "OpPCP",
    	rf."dataBaixa"::date as "dataBaixaPCP",
    	rf."horaMov"::time as "horaMovPCP",
    	rf."totPecasOPBaixadas" as "RealizadoPCP"
    from
    	"PCP".pcp.realizado_fase rf 
    where  
    	rf."dataBaixa"::date <= %s and codFase in (401, 1) ;
        """

    sqlMovEntradaEstoque = """
        select rf."codEngenharia",
    	rf.numeroop ,
    	rf.codfase:: varchar as "codFase", rf."seqRoteiro" , rf."dataBaixa"::date ,  rf."horaMov"::time,
    	rf."totPecasOPBaixadas" as "Realizado", rf."descOperMov" as operador, rf.chave 
    from
    	"PCP".pcp.realizado_fase rf 
    where 
    	rf."dataBaixa"::date >= %s 
    	and rf."dataBaixa"::date <= %s and codFase in (236, 449) ;
        """

    conn = ConexaoPostgreWms.conexaoEngine()
    MovEntradaEstoque = pd.read_sql(sqlMovEntradaEstoque, conn, params=(dataMovFaseIni, dataMovFaseFim,))
    MovEntradaEstoque['OpPCP'] = MovEntradaEstoque['numeroop'].apply(lambda x: x if x.endswith('-001') else x[:-4] + '-001')

    MovPCP = pd.read_sql(sqlMovPCP, conn, params=(dataMovFaseFim,))

    leadTime = pd.merge(MovEntradaEstoque,MovPCP,on='OpPCP',how='left')

    # Verifica e converte para datetime se necessário
    leadTime['dataBaixaPCP'] = pd.to_datetime(leadTime['dataBaixaPCP'], errors='coerce')
    leadTime['horaMovPCP'] = pd.to_datetime(leadTime['horaMovPCP'], format='%H:%M:%S', errors='coerce').dt.time
    leadTime['dataBaixa'] = pd.to_datetime(leadTime['dataBaixa'], errors='coerce')
    leadTime['horaMov'] = pd.to_datetime(leadTime['horaMov'], format='%H:%M:%S', errors='coerce').dt.time
    leadTime['LeadTime(diasCorridos)'] = (leadTime['dataBaixa'] - leadTime['dataBaixaPCP']).dt.days

    # Converter para string usando o formato desejado
    leadTime['dataBaixaPCP'] = leadTime['dataBaixaPCP'].dt.strftime('%Y-%m-%d')
    leadTime['dataBaixa'] = leadTime['dataBaixa'].dt.strftime('%Y-%m-%d')
    leadTime['horaMovPCP'] = leadTime['horaMovPCP'].apply(lambda x: x.strftime('%H:%M:%S') if pd.notnull(x) else None)
    leadTime['horaMov'] = leadTime['horaMov'].apply(lambda x: x.strftime('%H:%M:%S') if pd.notnull(x) else None)

    sqlNomeEngenharia = """
    select ic."codItemPai"::varchar , max(ic.nome)::varchar as nome from "PCP".pcp.itens_csw ic where ("codItemPai" like '1%') or ("codItemPai" like '5%') or ("codItemPai" like '2%') group by "codItemPai"
    """
    NomeEngenharia = pd.read_sql(sqlNomeEngenharia,conn)

    NomeEngenharia['codEngenharia'] = NomeEngenharia.apply(
        lambda r: '0' + r['codItemPai'] + '-0' if (r['codItemPai'].startswith('1') )| (r['codItemPai'].startswith('2')) else r['codItemPai'] + '-0', axis=1)
    leadTime = pd.merge(leadTime,NomeEngenharia,on='codEngenharia',how='left')
    leadTime['categoria'] = leadTime['nome'].apply(mapear_categoria)

    leadTime = leadTime.groupby(["categoria"]).agg({"LeadTime(diasCorridos)":"mean"}).reset_index()


    return leadTime