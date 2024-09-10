import pandas as pd
from models import PlanoClass
import fastparquet as fp
from connection import ConexaoBanco


class Faturamento():
    def __init__(self, dataInicial = None, dataFinal = None, tipoNotas = None, codigoPlano = None):
        self.dataInicial = dataInicial
        self.dataFinal = dataFinal
        self.tipoNotas = tipoNotas
        self.codigoPlano = codigoPlano


    def faturamentoPeriodo_Plano(self):
        '''Metodo para obter o faturamento de um determinado plano
        return:
        Dataframe [{'codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida',
                       'PrecoLiquido', 'codTipoNota'}]
        '''


        if self.codigoPlano == None:
            return pd.Dataframe([{'status':False, 'Mensagem':'Plano nao encontrado' }])
        else:
            plano = PlanoClass.Plano(self.codigoPlano)
            self.dataInicial = plano.obterDataInicioPlano()
            self.dataFinal = plano.obterDataFinalPlano()
            date = self.consultaArquivoFastVendas()
            date['status'] = True
            return date


    def consultaArquivoFastVendas(self):
        # Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile('./dados/pedidos.parquet')

        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()
        # Converter 'dataEmissao' para datetime
        df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)
        # Convertendo a string para datetime

        print(self.dataFinal)
        dataFatIni = pd.to_datetime(self.dataInicial)
        dataFatFinal = pd.to_datetime(self.dataFinal)

        # Filtrar as datas
        df_loaded['filtro'] = (df_loaded['dataPrevFat'] >= dataFatIni) & (df_loaded['dataPrevFat'] <= dataFatFinal)

        # 2 - Filtrar Apenas Pedidos Não Bloqueados
        pedidosBloqueados = self.Monitor_PedidosBloqueados()
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
        df_filtered['saldoPedido'] = df_filtered["qtdePedida"] - df_filtered["qtdeFaturada"] - df_filtered[
            "qtdeCancelada"]

        return df_filtered

    def Monitor_PedidosBloqueados(self):
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

