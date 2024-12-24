import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
import fastparquet as fp
from dotenv import load_dotenv, dotenv_values
import os
from models import PlanoClass, ProdutosClass, Meta
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
        #df_loaded['filtro4'] = df_loaded['dataPrevFat'] <= self.fimFat

        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['filtro2'] == True].reset_index()
        #print(df_loaded['filtro3'].drop_duplicates())
        df_loaded = df_loaded[df_loaded['filtro3'] == True].reset_index()
        #df_loaded = df_loaded[df_loaded['filtro4'] == True].reset_index()

        df_loaded = df_loaded.loc[:,
                    ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida','codTipoNota',
                     # 'StatusSugestao',
                     'PrecoLiquido']]


        df_loaded = pd.merge(df_loaded,produtos,on='codProduto',how='left')
        df_loaded['codItemPai'] = df_loaded['codItemPai'].astype(str)
        df_loaded['codItemPai'].fillna('-',inplace=True)

        # consultar = consultar.rename(columns={'StatusSugestao': 'Sugestao(Pedido)'})

        df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
        df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
        df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)
        df_loaded['valorVendido'] = df_loaded['qtdePedida'] * df_loaded['PrecoLiquido']
        # Convertendo para float antes de arredondar
        df_loaded['valorVendido'] = pd.to_numeric(df_loaded['valorVendido'], errors='coerce')

        # Aplicando o arredondamento
        df_loaded['valorVendido'] = df_loaded['valorVendido'].round(2)
        df_loaded = pd.merge(df_loaded,tiponotas,on='codTipoNota')


        if self.consideraPedidosBloqueados == 'nao':
            pedidosBloqueados = self.Monitor_PedidosBloqueados()
            df_loaded = pd.merge(df_loaded, pedidosBloqueados, on='codPedido', how='left')
            df_loaded['situacaobloq'].fillna('Liberado', inplace=True)
            df_loaded = df_loaded[df_loaded['situacaobloq'] == 'Liberado']



        conditions = [
            df_loaded['codItemPai'].str.startswith("102"),
            df_loaded['codItemPai'].str.startswith("202"),
            df_loaded['codItemPai'].str.startswith("104"),
            df_loaded['codItemPai'].str.startswith("204")
        ]
        choices = ["M.POLLO","M.POLLO", "PACO","PACO"]

        df_loaded['marca'] = np.select(conditions, choices, default="OUTROS")
        df_loaded = df_loaded[df_loaded['marca'] != 'OUTROS'].reset_index()
        groupByMarca = df_loaded.groupby(["marca"]).agg({"qtdePedida":"sum","valorVendido":'sum'}).reset_index()
        groupByCategoria = df_loaded.groupby(["marca","categoria"]).agg({"qtdePedida":"sum","valorVendido":'sum'}).reset_index()


        groupByCategoria['qtdePedida2'] = groupByCategoria['qtdePedida']
        groupByCategoria['valorVendido2'] = groupByCategoria['valorVendido']

        groupByCategoria['qtdePedida'] = groupByCategoria['qtdePedida'].apply(self.formatar_padraoInteiro)
        groupByCategoria['valorVendido'] = groupByCategoria['valorVendido'].apply(self.formatar_financeiro)

        groupByCategoria['marca2'] = groupByCategoria['marca']
        groupByCategoria['marca3'] = groupByCategoria['marca']
        groupByCategoria['marca4'] = groupByCategoria['marca']

        sqlMetaCategoria = """
                select
        			m."nomeCategoria" as "categoria",
        			m."metaPc" ,
        			m."metaFinanceira",
        			m."marca" 
        		from
        			"PCP".pcp."Meta_Categoria_Plano" m
        		where
        		    m."codPlano" = %s        
                """
        conn = ConexaoPostgreWms.conexaoEngine()
        sqlMetaCategoria = pd.read_sql(sqlMetaCategoria, conn, params=(self.codPlano,))
        groupByCategoria = pd.merge(groupByCategoria,sqlMetaCategoria,on=['categoria','marca'],how='left')
        groupByCategoria.fillna('-',inplace=True)

        groupByCategoria['metaPc'] = groupByCategoria['metaPc'].apply(self.formatar_padraoInteiro)
        groupByCategoria['metaFinanceira'] = groupByCategoria['metaFinanceira'].apply(self.formatar_financeiro)


        # Agregar valores por categoria
        groupByCategoria = groupByCategoria.groupby("categoria").agg({
            "marca": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'qtdePedida'])),
            "marca2": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'valorVendido'])),
            "marca3": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'metaPc'])),
            "marca4": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'metaFinanceira'])),
            "qtdePedida2": "sum",
            "valorVendido2":"sum"
        }).reset_index()
        #groupByCategoria = groupByCategoria.drop(columns=['marca2'])

        # Renomear colunas, se necessário
        groupByCategoria.rename(columns={"marca": "8.3-qtdVendido","marca2":"8.4-valorVendido",
                                         "marca3":"8.6-metaPcs",
                                         "marca4": "8.7-metaFinanceira",
                                         "categoria":"8.1-categoria",
                                         "valorVendido2":"8.3-TotalvalorVendido",
                                         "qtdePedida2":"8.2-TotalqtdePedida"}, inplace=True)

        totalVendasPeca = groupByMarca['qtdePedida'].sum()
        totalVendasReais = groupByMarca['valorVendido'].sum()

        groupByCategoria = groupByCategoria.sort_values(by=['8.2-TotalqtdePedida'],
                                                        ascending=False)  # escolher como deseja classificar
        groupByCategoria['8.5-precoMedioRealizado'] = (groupByCategoria['8.3-TotalvalorVendido'] / groupByCategoria['8.2-TotalqtdePedida']).round(2)

        groupByCategoria['8.2-TotalqtdePedida'] = groupByCategoria['8.2-TotalqtdePedida'].apply(self.formatar_padraoInteiro)

        groupByCategoria['8.3-TotalvalorVendido'] = groupByCategoria['8.3-TotalvalorVendido'].apply(self.formatar_financeiro)
        groupByCategoria['8.5-precoMedioRealizado'] = groupByCategoria['8.5-precoMedioRealizado'].apply(self.formatar_financeiro)



        metas = Meta.Meta(self.codPlano)

        metasDataFrame = metas.consultaMetaGeral()
        if metasDataFrame.empty:
            metasDataFrame = pd.DataFrame({
                'marca':['M.POLLO','PACO']
                ,'metaPecas':['0','0']
                , 'metaFinanceira': ['0', '0']

            })
            groupByMarca = pd.merge(groupByMarca,metasDataFrame,on='marca',how='left')
            totalMetasPeca = '0'
            totalMetaFinanceira = 'R$ 0,00'
        else:
            metasDataFrame = metasDataFrame.loc[:,
                        ['marca', 'metaFinanceira', 'metaPecas']]
            groupByMarca = pd.merge(groupByMarca, metasDataFrame, on='marca', how='left')
            totalMetasPeca = metasDataFrame['metaPecas'].str.replace('.','').astype(int).sum()
            # Convertendo para float e somando
            totalMetaFinanceira = metasDataFrame[metasDataFrame['marca']=='TOTAL'].reset_index()
            totalMetaFinanceira = totalMetaFinanceira['metaFinanceira'][0]

        # Convertendo para float antes de arredondar
        groupByMarca['valorVendido'] = pd.to_numeric(groupByMarca['valorVendido'], errors='coerce')
        # Aplicando o arredondamento
        groupByMarca['valorVendido'] = groupByMarca['valorVendido'].round(2)
        groupByMarca['precoMedioRealizado'] = (groupByMarca['valorVendido'] / groupByMarca['qtdePedida']).round(2)

        groupByMarca['precoMedioRealizado'] = groupByMarca['precoMedioRealizado'].apply(self.formatar_financeiro)
        groupByMarca['valorVendido'] = groupByMarca['valorVendido'].apply(self.formatar_financeiro)
        groupByMarca['qtdePedida'] = groupByMarca['qtdePedida'].apply(self.formatar_padraoInteiro)

        totalPrecoMedio = totalVendasReais/totalVendasPeca

        # Cria a linha de total
        total = pd.DataFrame([{
            'marca': 'TOTAL',
            'metaPecas': f'{totalMetasPeca:,.0f}'.replace(",", "X").replace("X", "."),
            'metaFinanceira': totalMetaFinanceira,
            'qtdePedida':f'{totalVendasPeca:,.0f}'.replace(",", "X").replace("X", "."),
            'valorVendido' : f'R$ {totalVendasReais:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),
            'precoMedioRealizado':f'R$ {totalPrecoMedio:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        }])

        # Concatena o total ao DataFrame original
        groupByMarca = pd.concat([groupByMarca, total], ignore_index=True)

        semanaAtual = plano.obterSemanaAtual()
        semanaAtualFat = plano.obterSemanaAtualFat()

        data = {
                '1- Intervalo Venda do Plano:': f'{self.iniVendas} - {self.fimVendas}',
                '2- Semanas de Venda':f'{plano.obterNumeroSemanasVendas()} semanas',
                '3- Semana de Venda Atual':f'{semanaAtual}',
                '4- Intervalo Faturamento do Plano:': f'{self.iniFat} - {self.fimFat}',
                '5- Semanas de Faturamento': f'{plano.obterNumeroSemanasFaturamento()} semanas',
                '6- Semana de Faturamento Atual': f'{semanaAtualFat}',
                '7- Detalhamento:': groupByMarca.to_dict(orient='records'),
                '8- DetalhamentoCategoria': groupByCategoria.to_dict(orient='records')
            }
        return pd.DataFrame([data])


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

    def formatar_financeiro(self,valor):
        try:
            return f'R$ {valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível

    def formatar_padraoInteiro(self,valor):
        try:
            return f'{valor:,.0f}'.replace(",", "X").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível


    def vendasPorSku(self):
        '''Metodo que disponibiliza as vendas a nivel de sku do Plano'''


        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        # Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile(f'{caminhoAbsoluto}/dados/pedidos.parquet')

        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()

        plano = PlanoClass.Plano(self.codPlano)
        self.iniVendas, self.fimVendas = plano.pesquisarInicioFimVendas()
        self.iniFat, self.fimFat = plano.pesquisarInicioFimFat()

        produtos = ProdutosClass.Produto().consultaItensReduzidos()
        produtos.rename(
            columns={'codigo': 'codProduto'},
            inplace=True)



        tiponotas = plano.pesquisarTipoNotasPlano()

        df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)
        df_loaded['filtro'] = df_loaded['dataEmissao'] >= self.iniVendas
        df_loaded['filtro2'] = df_loaded['dataEmissao'] <= self.fimVendas
        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['filtro2'] == True].reset_index()
        df_loaded = df_loaded.loc[:,
                    ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida','codTipoNota',
                     # 'StatusSugestao',
                     'PrecoLiquido']]


        df_loaded = pd.merge(df_loaded,produtos,on='codProduto',how='left')
        df_loaded['codItemPai'] = df_loaded['codItemPai'].astype(str)
        df_loaded['codItemPai'].fillna('-',inplace=True)

        # consultar = consultar.rename(columns={'StatusSugestao': 'Sugestao(Pedido)'})

        df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
        df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
        df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)
        df_loaded['valorVendido'] = df_loaded['qtdePedida'] * df_loaded['PrecoLiquido']
        # Convertendo para float antes de arredondar
        df_loaded['valorVendido'] = pd.to_numeric(df_loaded['valorVendido'], errors='coerce')

        # Aplicando o arredondamento
        df_loaded['valorVendido'] = df_loaded['valorVendido'].round(2)
        df_loaded = pd.merge(df_loaded,tiponotas,on='codTipoNota')


        if self.consideraPedidosBloqueados == 'nao':
            pedidosBloqueados = self.Monitor_PedidosBloqueados()
            df_loaded = pd.merge(df_loaded, pedidosBloqueados, on='codPedido', how='left')
            df_loaded['situacaobloq'].fillna('Liberado', inplace=True)
            df_loaded = df_loaded[df_loaded['situacaobloq'] == 'Liberado']



        conditions = [
            df_loaded['codItemPai'].str.startswith("102"),
            df_loaded['codItemPai'].str.startswith("202"),
            df_loaded['codItemPai'].str.startswith("104"),
            df_loaded['codItemPai'].str.startswith("204")
        ]
        choices = ["M.POLLO","M.POLLO", "PACO","PACO"]

        df_loaded['marca'] = np.select(conditions, choices, default="OUTROS")
        df_loaded = df_loaded[df_loaded['marca'] != 'OUTROS'].reset_index()
        groupBy = df_loaded.groupby(["codProduto"]).agg({"marca":"first",
                                                         "nome":'first',
                                                         "categoria":'first',
                                                         "codCor":"first",
                                                         "codItemPai":'first',
                                                         "qtdePedida":"sum",
                                                         "valorVendido":'sum'}).reset_index()
        groupBy = groupBy.sort_values(by=['qtdePedida'],
                                                        ascending=False)  # escolher como deseja classificar
        groupBy['valorVendido'] = groupBy['valorVendido'].apply(self.formatar_financeiro)
        groupBy['qtdePedida'] = groupBy['qtdePedida'].apply(self.formatar_padraoInteiro)

        # Renomear colunas, se necessário
        groupBy.rename(columns={"codProduto":"codReduzido"}, inplace=True)


        return groupBy