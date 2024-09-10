import pandas as pd
from connection import ConexaoPostgreWms,ConexaoBanco
from models.Planejamento import plano
import fastparquet as fp
from datetime import datetime, timedelta

def SaldosAnterior(codigoPlano):
    planoAtual = plano.ConsultaPlano()
    planoAtual = planoAtual[planoAtual['codigo']==codigoPlano].reset_index()

    # 1 - Levantar a data de Inicio de vendas do plano atual
    IniVendas = planoAtual['inicioVenda'][0]
    data_datetime = datetime.strptime(IniVendas, '%Y-%m-%d')
    data_nova = data_datetime - timedelta(days=1)
    data_nova = data_nova.strftime('%Y-%m-%d')

    #2 - Pedidos anteriores:

    pedidos = Monitor_nivelSku(data_nova)


    # 3 - Filtrando os pedidos aprovados
    pedidosBloqueados = _PedidosBloqueados()
    pedidos = pd.merge(pedidos,pedidosBloqueados,on='codPedido',how='left')
    pedidos['situacaobloq'].fillna('Liberado',inplace=True)
    pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']


    #4 Filtrando somente os tipo de notas desejados

    sqlNotas = """
    select tnp."tipo nota" as "codTipoNota"  from "PCP".pcp."tipoNotaporPlano" tnp 
    where plano = %s
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    tipoNotas = pd.read_sql(sqlNotas,conn,params=(codigoPlano,))

    pedidos = pd.merge(pedidos,tipoNotas,on='codTipoNota')
    pedidos = pedidos.groupby("codItem").agg({"saldo":"sum"}).reset_index()
    pedidos = pedidos.sort_values(by=['saldo'], ascending=False)
    pedidos = pedidos[pedidos['saldo'] > 0].reset_index()
    return pedidos




def _PedidosBloqueados():
    consultacsw = """SELECT * FROM (
    SELECT top 300000 bc.codPedido, 'analise comercial' as situacaobloq  from ped.PedidoBloqComl  bc WHERE codEmpresa = 1  
    and bc.situacaoBloq = 1
    order by codPedido desc
    UNION 
    SELECT top 300000 codPedido, 'analise credito'as situacaobloq  FROM Cre.PedidoCreditoBloq WHERE Empresa  = 1  
    and situacao = 1
    order BY codPedido DESC) as D"""

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(consultacsw)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            consulta = pd.DataFrame(rows, columns=colunas)

        del rows
        return consulta


def Monitor_nivelSku(dataFim):
    #sql = """SELECT * FROM "PCP".backup.pedidos p """
    #conn = ConexaoPostgreWms.conexaoEngine()
    #df_loaded = pd.read_sql(sql,conn)

    # Carregar o arquivo Parquet
    parquet_file = fp.ParquetFile('./dados/pedidos.parquet')

    # Converter para DataFrame do Pandas
    df_loaded = parquet_file.to_pandas()

    # Converter 'dataEmissao' para datetime
    df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)

    # Convertendo a string para datetime
    data_datetime = pd.to_datetime(dataFim)
    # Subtraindo 100 dias
    data_inic = data_datetime - pd.DateOffset(days=100)

    # Filtrar as datas
    df_loaded['filtro'] = (df_loaded['dataEmissao'] <= data_datetime) & (df_loaded['dataEmissao'] >= data_inic)

    # 2 - Filtrar Apenas Pedidos Não Bloqueados
    pedidosBloqueados = Monitor_PedidosBloqueados()
    df_loaded = pd.merge(df_loaded,pedidosBloqueados,on='codPedido',how='left')
    df_loaded['situacaobloq'].fillna('Liberado',inplace=True)
    df_loaded = df_loaded[df_loaded['situacaobloq'] == 'Liberado']

    # Aplicar o filtro
    df_filtered = df_loaded[df_loaded['filtro']].reset_index(drop=True)

    # Selecionar colunas relevantes
    df_filtered = df_filtered.loc[:,
                  ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida',
                   'PrecoLiquido','codTipoNota']]

    # Convertendo colunas para numérico
    df_filtered['qtdeSugerida'] = pd.to_numeric(df_filtered['qtdeSugerida'], errors='coerce').fillna(0)
    df_filtered['qtdePedida'] = pd.to_numeric(df_filtered['qtdePedida'], errors='coerce').fillna(0)
    df_filtered['qtdeFaturada'] = pd.to_numeric(df_filtered['qtdeFaturada'], errors='coerce').fillna(0)
    df_filtered['qtdeCancelada'] = pd.to_numeric(df_filtered['qtdeCancelada'], errors='coerce').fillna(0)

    # Adicionando coluna 'codItem'
    df_filtered['codItem'] = df_filtered['codProduto']

    # Calculando saldo
    df_filtered['saldo'] = df_filtered["qtdePedida"] - df_filtered["qtdeFaturada"] - df_filtered["qtdeCancelada"]

    return df_filtered



def Pedidos_saldo(codigoPlano, codItem):
    planoAtual = plano.ConsultaPlano()
    planoAtual = planoAtual[planoAtual['codigo']==codigoPlano].reset_index()

    # 1 - Levantar a data de Inicio de vendas do plano atual
    IniVendas = planoAtual['inicioVenda'][0]
    data_datetime = datetime.strptime(IniVendas, '%Y-%m-%d')
    data_nova = data_datetime - timedelta(days=1)
    data_nova = data_nova.strftime('%Y-%m-%d')

    #2 - Pedidos anteriores:

    pedidos = Monitor_nivelSku(data_nova)

    # 3 - Filtrando os pedidos aprovados
    pedidosBloqueados = _PedidosBloqueados()
    pedidos = pd.merge(pedidos,pedidosBloqueados,on='codPedido',how='left')
    pedidos['situacaobloq'].fillna('Liberado',inplace=True)
    pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']
    pedidos = pedidos[pedidos['codItem'] == str(codItem)]


    #4 Filtrando somente os tipo de notas desejados

    sqlNotas = """
    select tnp."tipo nota" as "codTipoNota"  from "PCP".pcp."tipoNotaporPlano" tnp 
    where plano = %s
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    tipoNotas = pd.read_sql(sqlNotas,conn,params=(codigoPlano,))

    pedidos = pd.merge(pedidos,tipoNotas,on='codTipoNota')
    pedidos = pedidos[pedidos['saldo'] > 0].reset_index()
    pedidos.drop(['codProduto','index','codProduto'], axis=1, inplace=True)
    pedidos.rename(columns={'saldo': 'saldoEntregar'}, inplace=True)

    return pedidos

def FaturamentoPlano(codigoPlano):
    planoAtual = plano.ConsultaPlano()
    planoAtual = planoAtual[planoAtual['codigo']==codigoPlano].reset_index()

    # Levantar as data de início e fim do faturamento:
    IniFat = planoAtual['inicoFat'][0]
    FimFat = planoAtual['finalFat'][0]

    #Filtrando somente os pedidos
    pedidos = Monitor_nivelSkuFaturamento(IniFat,FimFat)

    # 3 - Filtrando os pedidos aprovados
    pedidosBloqueados = _PedidosBloqueados()
    pedidos = pd.merge(pedidos, pedidosBloqueados, on='codPedido', how='left')
    pedidos['situacaobloq'].fillna('Liberado', inplace=True)
    pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']

    # 4 Filtrando somente os tipo de notas desejados

    sqlNotas = """
        select tnp."tipo nota" as "codTipoNota"  from "PCP".pcp."tipoNotaporPlano" tnp 
        where plano = %s
        """

    conn = ConexaoPostgreWms.conexaoEngine()
    tipoNotas = pd.read_sql(sqlNotas, conn, params=(codigoPlano,))

    pedidos = pd.merge(pedidos, tipoNotas, on='codTipoNota')
    pedidos = pedidos.groupby("codItem").agg({"qtdeFaturada": "sum"}).reset_index()
    pedidos = pedidos.sort_values(by=['qtdeFaturada'], ascending=False)
    pedidos = pedidos[pedidos['qtdeFaturada'] > 0].reset_index()
    return pedidos






def Monitor_PedidosBloqueados():
    consultacsw = """SELECT * FROM (
    SELECT top 300000 bc.codPedido, 'analise comercial' as situacaobloq  from ped.PedidoBloqComl  bc WHERE codEmpresa = 1  
    and bc.situacaoBloq = 1
    order by codPedido desc
    UNION 
    SELECT top 300000 codPedido, 'analise credito'as situacaobloq  FROM Cre.PedidoCreditoBloq WHERE Empresa  = 1  
    and situacao = 1
    order BY codPedido DESC) as D"""

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(consultacsw)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            consulta = pd.DataFrame(rows, columns=colunas)

        del rows
        return consulta


def Monitor_nivelSkuFaturamento(dataFatIni, dataFatFinal):
    # Carregar o arquivo Parquet
    parquet_file = fp.ParquetFile('./dados/pedidos.parquet')

    # Converter para DataFrame do Pandas
    df_loaded = parquet_file.to_pandas()

    # Converter 'dataEmissao' para datetime
    df_loaded['dataPrevFat'] = pd.to_datetime(df_loaded['dataPrevFat'], errors='coerce', infer_datetime_format=True)

    # Convertendo a string para datetime
    dataFatIni = pd.to_datetime(dataFatIni)
    dataFatFinal = pd.to_datetime(dataFatFinal)

    # Filtrar as datas
    df_loaded['filtro'] = (df_loaded['dataPrevFat'] >= dataFatIni) & (df_loaded['dataPrevFat'] <= dataFatFinal)

    # 2 - Filtrar Apenas Pedidos Não Bloqueados
    pedidosBloqueados = Monitor_PedidosBloqueados()
    df_loaded = pd.merge(df_loaded, pedidosBloqueados, on='codPedido', how='left')
    df_loaded['situacaobloq'].fillna('Liberado', inplace=True)
    df_loaded = df_loaded[df_loaded['situacaobloq'] == 'Liberado']

    # Aplicar o filtro
    df_filtered = df_loaded[df_loaded['filtro']].reset_index(drop=True)

    # Selecionar colunas relevantes
    df_filtered = df_filtered.loc[:,
                  ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida',
                   'PrecoLiquido', 'codTipoNota']]

    # Convertendo colunas para numérico
    df_filtered['qtdeSugerida'] = pd.to_numeric(df_filtered['qtdeSugerida'], errors='coerce').fillna(0)
    df_filtered['qtdePedida'] = pd.to_numeric(df_filtered['qtdePedida'], errors='coerce').fillna(0)
    df_filtered['qtdeFaturada'] = pd.to_numeric(df_filtered['qtdeFaturada'], errors='coerce').fillna(0)
    df_filtered['qtdeCancelada'] = pd.to_numeric(df_filtered['qtdeCancelada'], errors='coerce').fillna(0)

    # Adicionando coluna 'codItem'
    df_filtered['codItem'] = df_filtered['codProduto']

    # Calculando saldo
    df_filtered['saldoPedido'] = df_filtered["qtdePedida"] - df_filtered["qtdeFaturada"] - df_filtered["qtdeCancelada"]

    return df_filtered
