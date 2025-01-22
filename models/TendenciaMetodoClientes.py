import pandas as pd
import fastparquet as fp
from dotenv import load_dotenv, dotenv_values
import os
from models import PlanoClass, ProdutosClass, Vendas
import numpy as np


class TendenciaMetodoClientes():

    def __init__(self, codPlanoAnterior = None, empresa = 1, consideraPedidosBloqueados = 'nao'):

        self.codPlanoAnterior = codPlanoAnterior
        self.empresa = empresa
        self.consideraPedidosBloqueados = consideraPedidosBloqueados

    def clientesAtendidosMarca_Empresa(self):
        '''Metodo que consulta o numero de Clientes Atendidos no Plano Comparativo '''

        pedidos = self.listagem_pedidos()
        pedidos['Regiao'] = pedidos['nomeEstado'].apply(self.obter_regiao)

        # Encontrando o disponivel :
        pedidos = pedidos.groupby(['marca','Regiao','nomeRepresentante']).agg(
            clientes_distintos=('nomeCliente', 'nunique'),  # Número de clientes distintos
            quantidadePlanoAnt=('qtdePedida', 'sum')  # Soma das quantidades pedidas
        ).reset_index()
        pedidos.rename(columns={'clientes_distintos': 'clientesAtendidosPlanoAnt'}, inplace=True)

        pedidos['Pçs/ClientePlanoAnt'] = pedidos['quantidadePlanoAnt']  / pedidos['clientesAtendidosPlanoAnt']
        pedidos['Pçs/ClientePlanoAnt'] = pedidos['Pçs/ClientePlanoAnt'].round().astype(int)
        pedidos['quantidadePlanoAnt'] = pedidos['quantidadePlanoAnt'].round().astype(int)

        return pedidos

    def listagem_pedidos(self):
        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        # 1.2 - Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile(f'{caminhoAbsoluto}/dados/pedidos.parquet')
        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()
        plano = PlanoClass.Plano(self.codPlanoAnterior)
        self.iniVendas, self.fimVendas = plano.pesquisarInicioFimVendas()
        self.iniFat, self.fimFat = plano.pesquisarInicioFimFat()
        produtos = ProdutosClass.Produto().consultaItensReduzidos()
        produtos.rename(
            columns={'codigo': 'codProduto'},
            inplace=True)
        tiponotas = plano.pesquisarTipoNotasPlano()
        df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)
        df_loaded['dataPrevFat'] = pd.to_datetime(df_loaded['dataPrevFat'], errors='coerce', infer_datetime_format=True)
        df_loaded['filtro'] = df_loaded['dataEmissao'] >= self.iniVendas
        df_loaded['filtro2'] = df_loaded['dataEmissao'] <= self.fimVendas
        df_loaded['filtro3'] = df_loaded['dataPrevFat'] >= self.iniFat
        df_loaded['filtro4'] = df_loaded['dataPrevFat'] <= self.fimFat
        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['filtro2'] == True].reset_index()
        # print(df_loaded['filtro3'].drop_duplicates())
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])
        df_loaded = df_loaded[df_loaded['filtro3'] == True].reset_index()
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])
        df_loaded = df_loaded[df_loaded['filtro4'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['situacaoPedido'] != '9']
        df_loaded = pd.merge(df_loaded, produtos, on='codProduto', how='left')
        df_loaded['codItemPai'] = df_loaded['codItemPai'].astype(str)
        df_loaded['codItemPai'].fillna('-', inplace=True)
        # consultar = consultar.rename(columns={'StatusSugestao': 'Sugestao(Pedido)'})
        df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
        df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
        df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = df_loaded['qtdePedida'] - df_loaded['qtdeCancelada']
        df_loaded['valorVendido'] = df_loaded['qtdePedida'] * df_loaded['PrecoLiquido']
        # Convertendo para float antes de arredondar
        df_loaded['valorVendido'] = pd.to_numeric(df_loaded['valorVendido'], errors='coerce')
        # Aplicando o arredondamento
        df_loaded['valorVendido'] = df_loaded['valorVendido'].round(2)
        df_loaded = pd.merge(df_loaded, tiponotas, on='codTipoNota')
        if self.consideraPedidosBloqueados == 'nao':
            venda = Vendas.VendasAcom(self.codPlanoAnterior)
            pedidosBloqueados = venda.Monitor_PedidosBloqueados()
            df_loaded = pd.merge(df_loaded, pedidosBloqueados, on='codPedido', how='left')
            df_loaded['situacaobloq'].fillna('Liberado', inplace=True)
            df_loaded = df_loaded[df_loaded['situacaobloq'] == 'Liberado']
        conditions = [
            df_loaded['codItemPai'].str.startswith("102"),
            df_loaded['codItemPai'].str.startswith("202"),
            df_loaded['codItemPai'].str.startswith("104"),
            df_loaded['codItemPai'].str.startswith("204")
        ]
        choices = ["M.POLLO", "M.POLLO", "PACO", "PACO"]
        df_loaded['marca'] = np.select(conditions, choices, default="OUTROS")
        df_loaded = df_loaded[df_loaded['marca'] != 'OUTROS']
        return df_loaded

    def obter_regiao(self,nome_estado):
        # Dicionário de mapeamento estado -> região
        regioes = {
            'AC': 'NORTE', 'AP': 'NORTE', 'AM': 'NORTE', 'PA': 'NORTE', 'RO': 'NORTE', 'RR': 'NORTE', 'TO': 'NORTE',
            'AL': 'NORDESTE', 'BA': 'NORDESTE', 'CE': 'NORDESTE', 'MA': 'NORDESTE',
            'PB': 'NORDESTE', 'PE': 'NORDESTE', 'PI': 'NORDESTE', 'RN': 'NORDESTE', 'SE': 'NORDESTE',
            'DF': 'CENTRO-OESTE', 'GO': 'CENTRO-OESTE', 'MS': 'CENTRO-OESTE', 'MT': 'CENTRO-OESTE',
            'ES': 'SUDESTE', 'MG': 'SUDESTE', 'RJ': 'SUDESTE', 'SP': 'SUDESTE',
            'PR': 'SUL', 'RS': 'SUL', 'SC': 'SUL'
        }
        # Retorna a região correspondente ao estado
        return regioes.get(nome_estado.upper(), 'REGIÃO DESCONHECIDA')