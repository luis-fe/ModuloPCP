import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
import fastparquet as fp
from dotenv import load_dotenv, dotenv_values
import os
from models import PlanoClass
class VendasAcom():
    '''Classe utilizada para acompanhar as vendas de acordo com o plano'''


    def __init__(self, codPlano = None, empresa = '1', consideraPedidosBloqueados = 'nao'):

        self.codPlano = codPlano
        self.empresa = empresa
        self.consideraPedidosBloqueados = consideraPedidosBloqueados


    def vendasGeraisPorPlano(self):
        '''metodo que carrega as vendas gerais por plano '''

        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        # Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile(f'{caminhoAbsoluto}/dados/pedidos.parquet')

        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()

        plano = PlanoClass.Plano(self.codPlano)
        self.iniVendas, self.fimVendas = plano.pesquisarInicioFimVendas()

        tiponotas = plano.pesquisarTipoNotasPlano()

        df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)
        df_loaded['filtro'] = df_loaded['dataEmissao'] >= self.iniVendas
        df_loaded['filtro2'] = df_loaded['dataEmissao'] >= self.fimVendas

        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['filtro2'] == True].reset_index()

        df_loaded = df_loaded.loc[:,
                    ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida','codTipoNota',
                     # 'StatusSugestao',
                     'PrecoLiquido']]
        # consultar = consultar.rename(columns={'StatusSugestao': 'Sugestao(Pedido)'})

        df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
        df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
        df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)

        df_loaded = pd.merge(df_loaded,tiponotas,on='codTipoNota')


        if self.consideraPedidosBloqueados == 'nao':
            pedidosBloqueados = self.Monitor_PedidosBloqueados()
            df_loaded = pd.merge(df_loaded, pedidosBloqueados, on='codPedido', how='left')
            df_loaded['situacaobloq'].fillna('Liberado', inplace=True)
            df_loaded = df_loaded[df_loaded['situacaobloq'] == 'Liberado']


        totalVendasPeca = df_loaded['qtdePedida'] .sum()

        resultado = pd.DataFrame([{'Intervalo Venda do Plano':f'{self.iniVendas} - {self.fimVendas}',
                                   'Total Vendas Peca':totalVendasPeca}])

        return resultado

    def capaPedidos(self):
        empresa = "'" + str(self.empresa) + "'"
        self.iniVendas, self.fimVendas = PlanoClass.Plano(self.codPlano).pesquisarInicioFimVendas()


        sqlCswCapaPedidos = """
        SELECT   
            dataEmissao, 
            convert(varchar(9), codPedido) as codPedido,
            (select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli,
            codTipoNota, 
            dataPrevFat, 
            convert(varchar(9),codCliente) as codCliente, 
            codRepresentante, 
            descricaoCondVenda, 
            vlrPedido as vlrSaldo, 
            qtdPecasFaturadas
        FROM 
            Ped.Pedido p
        where 
            codEmpresa = """ + empresa + """  and  dataEmissao >= '""" + self.iniVendas + """' and dataEmissao <= '""" + self.fimVendas + """' and codTipoNota in (""" + tiponota + """)  """

        with ConexaoBanco.Conexao2() as conn:
            consulta = pd.read_sql(sqlCswCapaPedidos, conn)
        return consulta

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