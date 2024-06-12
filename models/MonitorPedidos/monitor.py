'''
Arquivo models- MonitorPedidos que conecta o monitor do pré faturamento
'''

import pandas as pd
import datetime
import pytz
from connection import ConexaoBanco, ConexaoPostgreWms
import fastparquet as fp

def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y')
    return agora

#Carregando a Capa de pedidos do CSW : Opcao 1: pela dataEmissao
def Monitor_CapaPedidos(empresa, iniVenda, finalVenda, tiponota):
    empresa = "'"+str(empresa)+"'"
    sqlCswCapaPedidos = "SELECT   dataEmissao, convert(varchar(9), codPedido) as codPedido, "\
    "(select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli, "\
    " codTipoNota, dataPrevFat, convert(varchar(9),codCliente) as codCliente, codRepresentante, descricaoCondVenda, vlrPedido as vlrSaldo, qtdPecasFaturadas "\
    " FROM Ped.Pedido p"\
    " where codEmpresa = "+empresa+"  and  dataEmissao >= '" + iniVenda + "' and dataEmissao <= '" + finalVenda + "' and codTipoNota in (" + tiponota + ")  "
    with ConexaoBanco.Conexao2() as conn:
        consulta = pd.read_sql(sqlCswCapaPedidos, conn)
    return consulta

#Carregando a Capa de pedidos do CSW : Opcao 2: pela dataPrevisao
def Monitor_CapaPedidosDataPrev(empresa, iniVenda, finalVenda, tiponota):
    empresa = "'"+str(empresa)+"'"

    sqlCswCapaPedidosDataPrev = "SELECT   dataEmissao, convert(varchar(9), codPedido) as codPedido, "\
    "(select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli, "\
    " codTipoNota, dataPrevFat, convert(varchar(9),codCliente) as codCliente, codRepresentante, descricaoCondVenda, vlrPedido as vlrSaldo, qtdPecasFaturadas "\
    " FROM Ped.Pedido p"\
    " where codEmpresa = "+empresa+"  and  dataPrevFat >= '" + iniVenda + "' and dataPrevFat <= '" + finalVenda + "' and codTipoNota in (" + tiponota + ")  "


    with ConexaoBanco.Conexao2() as conn:
     consulta = pd.read_sql(sqlCswCapaPedidosDataPrev, conn)

    return consulta

# Verfiicando se o pedido nao está bloqueado :
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
        consulta = pd.read_sql(consultacsw, conn)
    return consulta

#Carregando os Pedidos a nivel Sku
def Monitor_nivelSku(datainicio):
    # Carregar o arquivo Parquet
    parquet_file = fp.ParquetFile('./dados/pedidos.parquet')

    # Converter para DataFrame do Pandas
    df_loaded = parquet_file.to_pandas()

    df_loaded['dataEmissao']= pd.to_datetime(df_loaded['dataEmissao'],errors='coerce', infer_datetime_format=True)
    teste = datainicio
    df_loaded['filtro'] = df_loaded['dataEmissao'] >= teste
    df_loaded = df_loaded[df_loaded['filtro']==True].reset_index()
    df_loaded = df_loaded.loc[:, ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada','qtdeSugerida',#'StatusSugestao',
                                   'PrecoLiquido']]
    #consultar = consultar.rename(columns={'StatusSugestao': 'Sugestao(Pedido)'})

    df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
    df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
    df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
    df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)


    return df_loaded

#EstoquePorSku
def EstoqueSKU():

    consultasqlCsw = """select dt.reduzido as codProduto, SUM(dt.estoqueAtual) as estoqueAtual, sum(estReservPedido) as estReservPedido from
    (select codItem as reduzido, estoqueAtual,estReservPedido  from est.DadosEstoque where codEmpresa = 1 and codNatureza = 5 and estoqueAtual > 0)dt
    group by dt.reduzido
     """
    with ConexaoBanco.Conexao2() as conn:
        consulta = pd.read_sql(consultasqlCsw, conn)
    return consulta

#ObtendoEntregasSolicitadas
def ObtendoEntregasSolicitadas():
    consultasqlCsw ="""select top 100000 
                                         CAST(codPedido as varchar) as codPedido, 
                                         numeroEntrega as entregas_Solicitadas from asgo_ped.Entregas where 
                                         codEmpresa = 1  order by codPedido desc"""
    with ConexaoBanco.Conexao2() as conn:
        consulta = pd.read_sql(consultasqlCsw, conn)
    return consulta

#Entregas_Enviados
def ObtendoEntregas_Enviados():
    consultasqlCsw ="""
    select  top 300000 codPedido, count(codNumNota) as entregas_enviadas, 
    max(dataFaturamento) as ultimo_fat from fat.NotaFiscal  where codEmpresa = 1 and codRepresentante
    not in ('200','800','300','600','700','511') and situacao = 2 and codpedido> 0 and dataFaturamento > '2020-01-01' group by codPedido order by codPedido desc
    """
    with ConexaoBanco.Conexao2() as conn:
        consulta = pd.read_sql(consultasqlCsw, conn)
    return consulta

#Obtendo os Sku - estrutura
def EstruturaSku():
    conn =ConexaoPostgreWms.conexaoEngine()
    consultar = pd.read_sql("""Select "codSKU" as "codProduto", "codItemPai", "codCor", "nomeSKU" from pcp."SKU" """,conn)
    conn.close()

    return consultar