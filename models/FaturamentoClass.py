import pandas as pd
from models import PlanoClass, ProdutosClass, Pedidos_CSW
import fastparquet as fp
from connection import ConexaoBanco, ConexaoPostgreWms
from datetime import datetime, timedelta
from dotenv import load_dotenv, dotenv_values
import os

class Faturamento():
    '''Classe que interagem com o faturamento'''
    def __init__(self, dataInicial = None, dataFinal = None, tipoNotas = None, codigoPlano = None, relacaoPartes = None):
        '''Construtor da classe'''

        self.dataInicial = dataInicial # dataInicial de faturamento
        self.dataFinal = dataFinal # dataFinal de faturamento
        self.tipoNotas = tipoNotas
        self.codigoPlano = codigoPlano
        self.relacaoPartes = relacaoPartes

        self.pedidoCsw = Pedidos_CSW.Pedidos_CSW()


    def pedidosBloqueados(self):
        '''Metodo que busca os pedidos bloqueados e retorna em um DataFrame '''

        self._pedidosBloqueados = self.pedidoCsw.pedidosBloqueados()







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

            #Obtendo a dataInicial e dataFinal do Plano
            self.dataInicial = plano.obterDataInicioFatPlano()
            self.dataFinal = plano.obterDataFinalFatPlano()




            pedidos = self.consultaArquivoFastVendas()

            pedidos['status'] = True
            # 3 - Filtrando os pedidos aprovados
            pedidos = pd.merge(pedidos, self._pedidosBloqueados, on='codPedido', how='left')
            pedidos['situacaobloq'].fillna('Liberado', inplace=True)
            pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']

            # 4 Filtrando somente os tipo de notas desejados


            tipoNotas = plano.pesquisarTipoNotasPlano()

            pedidos = pd.merge(pedidos, tipoNotas, on='codTipoNota')
            pedidos = pedidos.groupby("codItem").agg({"qtdeFaturada": "sum"}).reset_index()
            pedidos = pedidos.sort_values(by=['qtdeFaturada'], ascending=False)
            pedidos = pedidos[pedidos['qtdeFaturada'] > 0].reset_index()
            return pedidos

    def consultaArquivoFastVendas(self):
        '''Metodo utilizado para ler um arquivo do tipo parquet e converter em um DataFrame '''

        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        # Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile(f'{caminhoAbsoluto}/dados/pedidos.parquet')

        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()
        # Converter 'dataEmissao' para datetime
        df_loaded['dataPrevFat'] = pd.to_datetime(df_loaded['dataPrevFat'], errors='coerce', infer_datetime_format=True)

        # Convertendo a string para datetime
        dataFatIni = pd.to_datetime(self.dataInicial)
        dataFatFinal = pd.to_datetime(self.dataFinal)

        # Filtrar as datas
        df_loaded['filtro'] = (df_loaded['dataPrevFat'] >= dataFatIni) & (df_loaded['dataPrevFat'] <= dataFatFinal)




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




    def faturamentoPeriodo_Plano_PartesPeca(self):
        '''Metodo para obter o faturamento no periodo do plano , convertido em partes de peças (SEMIACABADOS)'''




        faturamento = self.faturamentoPeriodo_Plano()

        faturamentoPartes = pd.merge(faturamento,self.relacaoPartes,on='codItem')
        # Drop do codProduto
        faturamentoPartes.drop('codItem', axis=1, inplace=True)

        # Rename do redParte para codProduto
        faturamentoPartes.rename(columns={'redParte': 'codItem'}, inplace=True)
        faturamentoPartes.drop(['codProduto','codSeqTamanho','codSortimento'], axis=1, inplace=True)


        return faturamentoPartes

