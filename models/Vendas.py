import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
import fastparquet as fp
from dotenv import load_dotenv, dotenv_values
import os
from models import PlanoClass, ProdutosClass, Meta
class VendasAcom():
    '''Classe utilizada para acompanhar as vendas de acordo com o plano'''


    def __init__(self, codPlano = None, empresa = '1', consideraPedidosBloqueados = 'nao', codReduzido = None):

        self.codPlano = codPlano
        self.empresa = empresa
        self.consideraPedidosBloqueados = consideraPedidosBloqueados

        self.codReduzido = codReduzido

    def vendasGeraisPorPlano(self):
        '''metodo que carrega as vendas gerais por plano '''

        df_loaded = self.listagemPedidosSku()
        plano = PlanoClass.Plano(self.codPlano)

        # Encontrando o disponivel :
        disponivel = df_loaded.groupby(["codProduto"]).agg({
                                                        "marca":'first',
                                                         "qtdePedida": "sum",
                                                         "qtdeFaturada": 'sum'}).reset_index()
        disponivel.rename(columns={"codProduto":"codReduzido"}, inplace=True)
        estoque = ProdutosClass.Produto().estoqueNat5()
        emProcesso = ProdutosClass.Produto().emProducao()

        disponivel = pd.merge(disponivel, estoque, on='codReduzido', how='left')
        disponivel['estoqueAtual'].fillna(0, inplace=True)
        disponivel = pd.merge(disponivel, emProcesso, on='codReduzido', how='left')
        disponivel['emProcesso'].fillna(0, inplace=True)
        disponivel['disponivel'] = (disponivel['emProcesso'] + disponivel['estoqueAtual']) - (
                    disponivel['qtdePedida'] - disponivel['qtdeFaturada'])
        disponivel['faltaProgVendido'] = disponivel['disponivel'].where(disponivel['disponivel'] < 0, 0)

        disponivel = disponivel.groupby(["marca"]).agg({"faltaProgVendido": 'sum'}).reset_index()

        groupByMarca = df_loaded.groupby(["marca"]).agg({"qtdePedida":"sum","valorVendido":'sum',"qtdeFaturada":"sum"}).reset_index()
        groupByMarca = pd.merge(groupByMarca, disponivel,on='marca', how='left')

        groupByCategoria = df_loaded.groupby(["marca","categoria"]).agg({"qtdePedida":"sum","valorVendido":'sum',"qtdeFaturada":"sum"}).reset_index()


        groupByCategoria['qtdePedida2'] = groupByCategoria['qtdePedida']
        groupByCategoria['valorVendido2'] = groupByCategoria['valorVendido']
        groupByCategoria['qtdeFaturada2'] = groupByCategoria['qtdeFaturada']

        groupByCategoria['qtdePedida'] = groupByCategoria['qtdePedida'].apply(self.formatar_padraoInteiro)
        groupByCategoria['qtdeFaturada'] = groupByCategoria['qtdeFaturada'].apply(self.formatar_padraoInteiro)
        groupByCategoria['valorVendido'] = groupByCategoria['valorVendido'].apply(self.formatar_financeiro)


        groupByCategoria['marca2'] = groupByCategoria['marca']
        groupByCategoria['marca3'] = groupByCategoria['marca']
        groupByCategoria['marca4'] = groupByCategoria['marca']
        groupByCategoria['marca5'] = groupByCategoria['marca']

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
            "marca5": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'qtdeFaturada'])),
            "qtdePedida2": "sum",
            "valorVendido2":"sum",
            "qtdeFaturada2":'sum'
        }).reset_index()
        #groupByCategoria = groupByCategoria.drop(columns=['marca2'])

        # Renomear colunas, se necessário
        groupByCategoria.rename(columns={"marca": "8.5-qtdVendido",
                                         "marca2":"8.6-valorVendido",
                                         "marca3":"8.7-metaPcs",
                                         "marca5": "8.8-qtdeFaturada",
                                         "marca4": "8.9-metaFinanceira",
                                         "categoria":"8.1-categoria",
                                         "valorVendido2":"8.3-TotalvalorVendido",
                                         "qtdeFaturada2": "8.4-TotalFaturadoPcs",
                                         "qtdePedida2":"8.2-TotalqtdePedida"}, inplace=True)

        totalVendasPeca = groupByMarca['qtdePedida'].sum()
        totalVendasReais = groupByMarca['valorVendido'].sum()
        totalqtdeFaturada = groupByMarca['qtdeFaturada'].sum()
        totalfaltaProgVendido = groupByMarca['faltaProgVendido'].sum()

        groupByCategoria = groupByCategoria.sort_values(by=['8.2-TotalqtdePedida'],
                                                        ascending=False)  # escolher como deseja classificar
        groupByCategoria['8.5-precoMedioRealizado'] = (groupByCategoria['8.3-TotalvalorVendido'] / groupByCategoria['8.2-TotalqtdePedida']).round(2)

        groupByCategoria['8.2-TotalqtdePedida'] = groupByCategoria['8.2-TotalqtdePedida'].apply(self.formatar_padraoInteiro)
        groupByCategoria['8.4-TotalFaturadoPcs'] = groupByCategoria['8.4-TotalFaturadoPcs'].apply(self.formatar_padraoInteiro)

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
        groupByMarca['qtdeFaturada'] = groupByMarca['qtdeFaturada'].apply(self.formatar_padraoInteiro)
        groupByMarca['qtdeFaturada'].fillna('0',inplace=True)
        groupByMarca['faltaProgVendido'] = groupByMarca['faltaProgVendido'].apply(self.formatar_padraoInteiro)

        totalPrecoMedio = totalVendasReais/totalVendasPeca

        # Cria a linha de total
        total = pd.DataFrame([{
            'marca': 'TOTAL',
            'metaPecas': f'{totalMetasPeca:,.0f}'.replace(",", "X").replace("X", "."),
            'metaFinanceira': totalMetaFinanceira,
            'qtdePedida':f'{totalVendasPeca:,.0f}'.replace(",", "X").replace("X", "."),
            'faltaProgVendido': f'{totalfaltaProgVendido:,.0f}'.replace(",", "X").replace("X", "."),
            'qtdeFaturada': f'{totalqtdeFaturada:,.0f}'.replace(",", "X").replace("X", "."),
            'valorVendido' : f'R$ {totalVendasReais:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),
            'precoMedioRealizado':f'R$ {totalPrecoMedio:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        }])

        # Concatena o total ao DataFrame original
        groupByMarca = pd.concat([groupByMarca, total], ignore_index=True)
        groupByMarca.fillna('-',inplace=True)

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




    def Monitor_PedidosBloqueados(self):
        consultacsw = """
        SELECT 
            * FROM (
                    SELECT top 300000 
                        bc.codPedido, 
                        'analise comercial' as situacaobloq  
                    from 
                        ped.PedidoBloqComl  bc 
                    WHERE 
                        codEmpresa = 1  
                        and bc.situacaoBloq = 1
                    order by codPedido desc
                    UNION 
                    SELECT top 300000 
                        codPedido, 
                        'analise credito'as situacaobloq  
                    FROM 
                        Cre.PedidoCreditoBloq 
                    WHERE 
                        Empresa  = 1  
                        and situacao = 1
                    order BY 
                        codPedido DESC) as D
        """

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

        df_loaded = self.listagemPedidosSku()
        groupBy = df_loaded.groupby(["codProduto"]).agg({"marca":"first",
                                                         "nome":'first',
                                                         "categoria":'first',
                                                         "codCor":"first",
                                                         "codItemPai":'first',
                                                         "qtdePedida":"sum",
                                                         "qtdeFaturada":'sum',
                                                         "valorVendido":'sum',
                                                         "codSeqTamanho":'first',
                                                         "codPedido":'count'}).reset_index()
        groupBy = groupBy.sort_values(by=['qtdePedida'],
                                                        ascending=False)  # escolher como deseja classificar
        tam = ProdutosClass.Produto().get_tamanhos()
        groupBy['codSeqTamanho'] = groupBy['codSeqTamanho'].astype(str).str.replace('.0','')
        tam['codSeqTamanho'] = tam['codSeqTamanho'].astype(str).str.replace('.0','')
        groupBy = pd.merge(groupBy,tam,on='codSeqTamanho',how='left')


        # Renomear colunas, se necessário
        groupBy.rename(columns={"codProduto":"codReduzido","codPedido":"Ocorrencia em Pedidos"}, inplace=True)

        afv = ProdutosClass.Produto().statusAFV()
        estoque = ProdutosClass.Produto().estoqueNat5()
        emProcesso = ProdutosClass.Produto().emProducao()

        groupBy = pd.merge(groupBy, afv, on='codReduzido',how='left')
        groupBy['statusAFV'].fillna('Normal',inplace=True)
        groupBy = pd.merge(groupBy, estoque, on='codReduzido',how='left')
        groupBy['estoqueAtual'].fillna(0,inplace=True)
        groupBy = pd.merge(groupBy, emProcesso, on='codReduzido',how='left')
        groupBy['emProcesso'].fillna(0,inplace=True)

        groupBy['disponivel'] = (groupBy['emProcesso'] + groupBy['estoqueAtual'] ) - (groupBy['qtdePedida'] - groupBy['qtdeFaturada'] )

        groupBy['valorVendido'] = groupBy['valorVendido'].apply(self.formatar_financeiro)
        groupBy['qtdePedida'] = groupBy['qtdePedida'].apply(self.formatar_padraoInteiro)
        groupBy['qtdeFaturada'] = groupBy['qtdeFaturada'].apply(self.formatar_padraoInteiro)
        groupBy['disponivel'] = groupBy['disponivel'].apply(self.formatar_padraoInteiro)
        groupBy['emProcesso'] = groupBy['emProcesso'].apply(self.formatar_padraoInteiro)
        groupBy['estoqueAtual'] = groupBy['estoqueAtual'].apply(self.formatar_padraoInteiro)

        return groupBy

    def listagemPedidosSku(self):
        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        # 1.2 - Carregar o arquivo Parquet
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
        choices = ["M.POLLO", "M.POLLO", "PACO", "PACO"]
        df_loaded['marca'] = np.select(conditions, choices, default="OUTROS")
        df_loaded = df_loaded[df_loaded['marca'] != 'OUTROS']
        return df_loaded

    def detalhaPedidosSku(self):
        '''Metodo que consulta os pedidos do sku:
        codPedido, tipoNota, dataEmisao, dataPrev , cliente , qtdPedida
        '''

        df_loaded = self.listagemPedidosSku()
        df_loaded = df_loaded[df_loaded['codProduto'] == self.codReduzido]


        groupBy = df_loaded.groupby(["codPedido"]).agg({"marca":"first",
                                                         "qtdePedida":"sum",
                                                         "qtdeFaturada":'sum',
                                                         "valorVendido":'sum',
                                                        "codTipoNota":"first",
                                                        "dataEmissao":"first",
                                                        "dataPrevFat":"first"}).reset_index()
        groupBy['dataEmissao'] = pd.to_datetime(groupBy['dataEmissao'], format='%a, %d %b %Y %H:%M:%S %Z').dt.strftime('%Y-%m-%d')
        groupBy['dataPrevFat'] = pd.to_datetime(groupBy['dataPrevFat'], format='%a, %d %b %Y %H:%M:%S %Z').dt.strftime('%Y-%m-%d')

        groupBy = groupBy.sort_values(by=['qtdePedida'],
                                                        ascending=False)  # escolher como deseja classificar
        return groupBy


