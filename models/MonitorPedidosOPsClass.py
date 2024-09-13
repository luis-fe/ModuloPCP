import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
import fastparquet as fp


class MonitorPedidosOps():
    '''Classe criada para o Sistema de PCP gerenciar a FILA DE PEDIDOS emitidos para o grupo MPL'''

    def __init__(self, empresa, dataInicioVendas , dataFinalVendas, tipoDataEscolhida, dataInicioFat, dataFinalFat ,arrayRepresentantesExcluir = '',
                 arrayEscolherRepresentante = '', arrayescolhernomeCliente = ''):
        self.empresa = empresa
        self.dataInicioVendas = dataInicioVendas
        self.dataFinalVendas = dataFinalVendas
        self.tipoDataEscolhida = tipoDataEscolhida
        self.dataInicioFat = dataInicioFat
        self.dataFinalFat = dataFinalFat
        self.arrayRepresentantesExcluir = arrayRepresentantesExcluir
        self.arrayEscolherRepresentante = arrayEscolherRepresentante
        self.arrayescolhernomeCliente = arrayescolhernomeCliente

    def gerarMonitorPedidos(self):
        '''Metodo utilizado para gerar o monitor de pedidos
        return:
        DataFrame
        '''

        # 1 - Carregar Os pedidos (etapa 1)
        if self.tipoDataEscolhida == 'DataEmissao':
            pedidos = self.capaPedidos()
        else:
            pedidos = self.capaPedidosDataFaturamento()

        # 1.1 - Verificar se tem representantes a serem excuidos da analise
        if self.arrayRepresentantesExcluir != '':
            Representante_excluir = self.arrayRepresentantesExcluir.split(', ')
            pedidos = pedidos[~pedidos['codRepresentante'].astype(str).isin(Representante_excluir)]

        # 1.2 - Verificar se o filtro esta aplicado para representantes excluisvos
        if self.arrayEscolherRepresentante != '':
            escolherRepresentante = self.arrayEscolherRepresentante.split(', ')
            pedidos = pedidos[pedidos['codRepresentante'].astype(str).isin(escolherRepresentante)]

        # 1.3 - Verificar se o filtro esta aplicado para clientes excluisvos
        if self.arrayescolhernomeCliente != '':
            # escolhernomeCliente = escolhernomeCliente.split(', ')
            pedidos = pedidos[pedidos['nome_cli'].str.contains(self.arrayescolhernomeCliente, case=False, na=False)]

        statusSugestao = self.CapaSugestao()
        pedidos = pd.merge(pedidos, statusSugestao, on='codPedido', how='left')
        pedidos["StatusSugestao"].fillna('Nao Sugerido', inplace=True)
        pedidos["codSitSituacao"].fillna('0', inplace=True)

        # 2 - Filtrar Apenas Pedidos Não Bloqueados
        pedidosBloqueados = self.Monitor_PedidosBloqueados()
        pedidos = pd.merge(pedidos, pedidosBloqueados, on='codPedido', how='left')
        pedidos['situacaobloq'].fillna('Liberado', inplace=True)
        pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']

        # 3- Consulta de Embarques Enviados do pedido , utilizando a consulta de notas fiscais do ERP
        entregasFaturadas = self.ObtendoEntregas_Enviados()
        pedidos = pd.merge(pedidos, entregasFaturadas, on='codPedido', how='left')
        pedidos['entregas_enviadas'].fillna(0, inplace=True)

        # 4- Consulta de Embarques Solicitado pelo Cliente , informacao extraida do ERP
        entregasSolicitadas = self.ObtendoEntregasSolicitadas()
        pedidos = pd.merge(pedidos, entregasSolicitadas, on='codPedido', how='left')
        pedidos['entregas_Solicitadas'].fillna(0, inplace=True)

        # 5 - Explodir os pedidos no nivel sku
        if self.tipoDataEscolhida  == 'DataEmissao':
            sku = self.Monitor_nivelSku()
        else:
            sku = self.Monitor_nivelSkuPrev()

        estruturasku = EstruturaSku()

    def capaPedidos(self):
        '''Metodo que busca a capa dos pedidos no periodo de vendas'''
        tiponota = '1,2,3,4,5,6,7,8,10,24,92,201,1012,77,27,28,172,9998,66,67,233,237'  # Arrumar o Tipo de Nota 40
        empresa = "'" + str(self.empresa) + "'"

        sqlCswCapaPedidos = """
                                SELECT   
                                    dataEmissao, 
                                    convert(varchar(9), codPedido) as codPedido,
                                    (select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli, " \
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
                                    codEmpresa = """ + empresa + """
                                    and  dataEmissao >= '""" + self.dataInicioFat + """ 
                                    and dataEmissao <= '""" + self.dataFinalVendas + """' 
                                    and codTipoNota in (""" + tiponota + """)  """

        with ConexaoBanco.Conexao2() as conn:
            consulta = pd.read_sql(sqlCswCapaPedidos, conn)
        return consulta

    def capaPedidosDataFaturamento(self):
        '''Metodo que busca a capa dos pedidos no periodo pela dataPrevFat -- dataprevicaoFaturamento'''
        tiponota = '1,2,3,4,5,6,7,8,10,24,92,201,1012,77,27,28,172,9998,66,67,233,237'  # Arrumar o Tipo de Nota 40
        empresa = "'" + str(self.empresa) + "'"


        sqlCswCapaPedidosDataPrev = """
                                SELECT   
                                    dataEmissao, 
                                    convert(varchar(9), codPedido) as codPedido,
                                    (select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli, " \
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
                                    codEmpresa = """ + empresa + """
                                    and  dataPrevFat >= '""" + self.dataInicioVendas + """ 
                                    and dataPrevFat <= '""" + self.dataFinalFat + """' 
                                    and codTipoNota in (""" + tiponota + """)  """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlCswCapaPedidosDataPrev)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            del rows
            return consulta

    def CapaSugestao(self):
        consultasqlCsw = """
        SELECT 
            c.codPedido,
            situacaoSugestao as codSitSituacao ,
            case when (situacaoSugestao = 2 and dataHoraListagem>0) 
                then 'Sugerido(Em Conferencia)' 
                WHEN situacaoSugestao = 0 then 'Sugerido(Gerado)' WHEN situacaoSugestao = 2 then 'Sugerido(Em Conferencia)' 
                WHEN situacaoSugestao = 1 then 'Sugerido(Gerado)' else '' end StatusSugestao
        FROM 
            ped.SugestaoPed c WHERE c.codEmpresa = """+str(self.empresa)

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta

    def Monitor_PedidosBloqueados(self):
        consultacsw = """
        SELECT 
            * 
        FROM 
            (
                SELECT 
                    top 300000 bc.codPedido, 
                    'analise comercial' as situacaobloq  
                from 
                    ped.PedidoBloqComl  bc 
                WHERE 
                    codEmpresa = 1  
                    and bc.situacaoBloq = 1
                order by 
                    codPedido desc
                UNION 
                    SELECT 
                        top 300000 codPedido, 
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

    def ObtendoEntregas_Enviados(self):
        consultasqlCsw = """
            select  
                top 300000 codPedido, 
                count(codNumNota) as entregas_enviadas, 
                max(dataFaturamento) as ultimo_fat 
            from 
                fat.NotaFiscal  
            where 
                codEmpresa = 1 
                and codRepresentante
                not in ('200','800','300','600','700','511') 
                and situacao = 2 
                and codpedido> 0 
                and dataFaturamento > '2020-01-01' 
            group by 
                codPedido 
            order by 
                codPedido desc
        """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta

    def ObtendoEntregasSolicitadas(self):
        consultasqlCsw = """
        select 
            top 100000 CAST(codPedido as varchar) as codPedido, 
            numeroEntrega as entregas_Solicitadas 
        from 
            asgo_ped.Entregas 
        where 
            codEmpresa = 1  
        order by 
            codPedido desc
        """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta

    def Monitor_nivelSku(self):
        # Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile('./dados/pedidos.parquet')

        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()

        df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)
        teste = self.dataInicioVendas
        df_loaded['filtro'] = df_loaded['dataEmissao'] >= teste
        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        df_loaded = df_loaded.loc[:,
                    ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida',
                     # 'StatusSugestao',
                     'PrecoLiquido']]
        # consultar = consultar.rename(columns={'StatusSugestao': 'Sugestao(Pedido)'})

        df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
        df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
        df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)

        return df_loaded

    def Monitor_nivelSkuPrev(self):
        # Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile('./dados/pedidos.parquet')

        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()

        df_loaded['dataPrevFat'] = pd.to_datetime(df_loaded['dataPrevFat'], errors='coerce', infer_datetime_format=True)
        teste = self.dataInicioFat
        df_loaded['filtro'] = df_loaded['dataPrevFat'] >= teste
        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        df_loaded = df_loaded.loc[:,
                    ['codPedido', 'codProduto', 'qtdePedida', 'qtdeFaturada', 'qtdeCancelada', 'qtdeSugerida',
                     # 'StatusSugestao',
                     'PrecoLiquido']]
        # consultar = consultar.rename(columns={'StatusSugestao': 'Sugestao(Pedido)'})

        df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
        df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
        df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)

        return df_loaded

    def EstruturaSku(self):
        conn = ConexaoPostgreWms.conexaoEngine()
        consultar = pd.read_sql(
            """
            Select 
                "codigo" as "codProduto", 
                "codItemPai", 
                "codCor", 
                "nome" as "nome"
            from 
                pcp."itens_csw" 
            """, conn)

        return consultar

