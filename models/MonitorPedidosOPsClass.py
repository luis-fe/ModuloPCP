import numpy
import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
import fastparquet as fp
from models import EstoqueSkuClass
import pytz
from datetime import datetime, timedelta


class MonitorPedidosOps():
    '''Classe criada para o Sistema de PCP gerenciar a FILA DE PEDIDOS emitidos para o grupo MPL'''

    def __init__(self, empresa, dataInicioVendas , dataFinalVendas, tipoDataEscolhida, dataInicioFat, dataFinalFat ,arrayRepresentantesExcluir = '',
                 arrayEscolherRepresentante = '', arrayescolhernomeCliente = '', parametroClassificacao = None , filtroDataEmissaoIni = '' , filtroDataEmissaoFim = ''):
        self.empresa = empresa
        self.dataInicioVendas = dataInicioVendas
        self.dataFinalVendas = dataFinalVendas
        self.tipoDataEscolhida = tipoDataEscolhida
        self.dataInicioFat = dataInicioFat
        self.dataFinalFat = dataFinalFat
        self.arrayRepresentantesExcluir = arrayRepresentantesExcluir
        self.arrayEscolherRepresentante = arrayEscolherRepresentante
        self.arrayescolhernomeCliente = arrayescolhernomeCliente
        self.parametroClassificacao = parametroClassificacao
        self.filtroDataEmissaoIni = filtroDataEmissaoIni
        self.filtroDataEmissaoFim = filtroDataEmissaoFim

        self.descricaoArquivo = self.dataInicioFat + '_'+self.dataFinalFat

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

        estruturasku = self.EstruturaSku()
        estruturasku['CATEGORIA'] = 'canc'
        # 5.1 - Considerando somente a qtdePedida maior que 0
        pedidos = pd.merge(pedidos, sku, on='codPedido', how='left')
        pedidos = pd.merge(pedidos, estruturasku, on='codProduto', how='left')
        pedidos['QtdSaldo'] = pedidos['qtdePedida'] - pedidos['qtdeFaturada'] - pedidos['qtdeSugerida']
        pedidos['QtdSaldo'].fillna(0, inplace=True)
        pedidos['QtdSaldo'] = pedidos['QtdSaldo'].astype(int)

        # 6 Consultando n banco de dados do ERP o saldo de estoque
        estoque = EstoqueSkuClass.EstoqueSKU().consultaEstoqueConsolidadoPorReduzido_nat5()
        pedidos = pd.merge(pedidos, estoque, on='codProduto', how='left')

        # 7 Calculando a nova data de Previsao do pedido
        pedidos['dias_a_adicionar'] = pd.to_timedelta(pedidos['entregas_enviadas'] * 15,
                                                      unit='d')  # Converte a coluna de inteiros para timedelta
        pedidos['dataPrevAtualizada'] = pd.to_datetime(pedidos['dataPrevFat'], errors='coerce',
                                                       infer_datetime_format=True)
        pedidos['dataPrevAtualizada'] = pedidos['dataPrevAtualizada'] + pedidos['dias_a_adicionar']
        pedidos['dataPrevAtualizada'].fillna('-', inplace=True)

        # 8 -# Clasificando o Dataframe para analise
        pedidos = self.Classificacao(pedidos, self.parametroClassificacao)

        # 9 Contando o numero de ocorrencias acumulado do sku no DataFrame
        pedidos = pedidos[pedidos['vlrSaldo'] > 0]
        pedidos['Sku_acumula'] = pedidos.groupby('codProduto').cumcount() + 1

        # 10.1 Obtendo o Estoque Liquido para o calculo da necessidade
        pedidos['EstoqueLivre'] = pedidos['estoqueAtual'] - pedidos['estReservPedido']

        # 10.2 Obtendo a necessidade de estoque
        pedidos['Necessidade'] = pedidos.groupby('codProduto')['QtdSaldo'].cumsum()
        # 10.3 0 Obtendo a Qtd que antende para o pedido baseado no estoque
        # pedidos["Qtd Atende"] = pedidos.apply(lambda row: row['QtdSaldo']  if row['Necessidade'] <= row['EstoqueLivre'] else 0, axis=1)
        pedidos['Qtd Atende'] = pedidos['QtdSaldo'].where(pedidos['Necessidade'] <= pedidos['EstoqueLivre'], 0)
        # analise = pedidos[pedidos['codProduto']=='717411']
        # analise.to_csv('./dados/analise.csv')
        pedidos.loc[pedidos['qtdeSugerida'] > 0, 'Qtd Atende'] = pedidos['qtdeSugerida']
        pedidos['Qtd Atende'] = pedidos['Qtd Atende'].astype(int)

        # 11.1 Separando os pedidos a nivel pedido||engenharia||cor
        pedidos["Pedido||Prod.||Cor"] = pedidos['codPedido'].str.cat([pedidos['codItemPai'], pedidos['codCor']],
                                                                     sep='||')
        # 11.2  Calculando a necessidade a nivel de grade Pedido||Prod.||Cor
        pedidos['Saldo +Sugerido'] = pedidos['QtdSaldo'] + pedidos['qtdeSugerida']
        pedidos['Saldo Grade'] = pedidos.groupby('Pedido||Prod.||Cor')['Saldo +Sugerido'].transform('sum')
        # etapa11 = controle.salvarStatus_Etapa11(rotina, ip, etapa10, 'necessidade a nivel de grade Pedido||Prod.||Cor')#Registrar etapa no controlador

        # 12 obtendo a Qtd que antende para o pedido baseado no estoque e na grade
        pedidos['X QTDE ATENDE'] = pedidos.groupby('Pedido||Prod.||Cor')['Qtd Atende'].transform('sum')
        # pedidos['Qtd Atende por Cor'] = pedidos.apply(lambda row: row['Saldo +Sugerido'] if row['Saldo Grade'] == row['X QTDE ATENDE'] else 0, axis=1)
        pedidos['Qtd Atende por Cor'] = pedidos['Saldo +Sugerido'].where(
            pedidos['Saldo Grade'] == pedidos['X QTDE ATENDE'], 0)

        pedidos['Qtd Atende por Cor'] = pedidos['Qtd Atende por Cor'].astype(int)

        #13- Indicador de % que fecha no pedido a nivel de grade Pedido||Prod.||Cor'
        pedidos['Fecha Acumulado'] = pedidos.groupby('codPedido')['Qtd Atende por Cor'].cumsum().round(2)
        pedidos['Saldo +Sugerido_Sum'] = pedidos.groupby('codPedido')['Saldo +Sugerido'].transform('sum')
        pedidos['% Fecha Acumulado'] = (pedidos['Fecha Acumulado'] / pedidos['Saldo +Sugerido_Sum']).round(2) * 100
        pedidos['% Fecha Acumulado'] = pedidos['% Fecha Acumulado'].astype(str)
        pedidos['% Fecha Acumulado'] = pedidos['% Fecha Acumulado'].str.slice(0, 4)
        pedidos['% Fecha Acumulado'] = pedidos['% Fecha Acumulado'].astype(float)

        # 14 - Encontrando a Marca desejada
        pedidos['codItemPai'] = pedidos['codItemPai'].astype(str)
        pedidos['MARCA'] = pedidos['codItemPai'].str[:3]
        pedidos['MARCA'] = numpy.where(
            (pedidos['codItemPai'].str[:3] == '102') | (pedidos['codItemPai'].str[:3] == '202'), 'M.POLLO', 'PACO')

        # 16- Trazendo as configuracoes de % deistribuido configurado
        dadosConfPer = self.ConfiguracaoPercEntregas()

        # 16.1 Encontrando o numero restante de entregas
        pedidos['Entregas Restantes'] = pedidos['entregas_Solicitadas'] - pedidos['entregas_enviadas']
        # pedidos['Entregas Restantes'] = pedidos.apply(lambda row: 1 if row['entregas_Solicitadas'] <= row['entregas_enviadas'] else row['Entregas Restantes'], axis=1)
        pedidos.loc[pedidos['entregas_Solicitadas'] <= pedidos['entregas_enviadas'], 'Entregas Restantes'] = 1

        pedidos['Entregas Restantes'] = pedidos['Entregas Restantes'].astype(str)
        pedidos['Entregas Restantes'] = pedidos['Entregas Restantes'].str.replace('.0', '')
        pedidos = pd.merge(pedidos, dadosConfPer, on='Entregas Restantes', how='left')

        # 17 - Trazendo as configuracoes de categorias selecionadas e aplicando regras de categoria
        dadosCategoria = self.ConfiguracaoCategoria()
        pedidos = pd.merge(pedidos, dadosCategoria, on='CATEGORIA', how='left')
        pedidos.loc[pedidos['Status'] != '1', 'Qtd Atende por Cor'] = 0
        pedidos.loc[pedidos['Status'] != '1', 'Qtd Atende'] = 0

        # 18 - Encontrando no pedido o percentual que atende a distribuicao
        pedidos['% Fecha pedido'] = (pedidos.groupby('codPedido')['Qtd Atende por Cor'].transform('sum')) / (
            pedidos.groupby('codPedido')['Saldo +Sugerido'].transform('sum'))
        pedidos['% Fecha pedido'] = pedidos['% Fecha pedido'] * 100
        pedidos['% Fecha pedido'] = pedidos['% Fecha pedido'].astype(float).round(2)
        # etapa18 = controle.salvarStatus_Etapa18(rotina, ip, etapa17,'Encontrando no pedido o percentual que atende a distribuicao')  # Registrar etapa no controlador

        # 19 - Encontrando os valores que considera na ditribuicao
        pedidos['ValorMin'] = pedidos['ValorMin'].astype(float)
        pedidos['ValorMax'] = pedidos['ValorMax'].astype(float)
        condicoes = [(pedidos['% Fecha pedido'] >= pedidos['ValorMin']) &
                     (pedidos['% Fecha pedido'] <= pedidos['ValorMax']),
                     (pedidos['% Fecha pedido'] > pedidos['ValorMax']) &
                     (pedidos['% Fecha Acumulado'] <= pedidos['ValorMax']),
                     (pedidos['% Fecha pedido'] > pedidos['ValorMax']) &
                     (pedidos['% Fecha Acumulado'] > pedidos['ValorMax']),
                     (pedidos['% Fecha pedido'] < pedidos['ValorMin'])
                     # adicionar mais condições aqui, se necessário
                     ]
        valores = ['SIM', 'SIM', 'SIM(Redistribuir)', 'NAO']  # definir os valores correspondentes
        pedidos['Distribuicao'] = numpy.select(condicoes, valores, default=True)

        # função para avaliar cada grupo
        def avaliar_grupo(df_grupo):
            return len(set(df_grupo)) == 1

        df_resultado = pedidos.loc[:, ['Pedido||Prod.||Cor', 'Distribuicao']]
        df_resultado = df_resultado.groupby('Pedido||Prod.||Cor')['Distribuicao'].apply(avaliar_grupo).reset_index()
        df_resultado.columns = ['Pedido||Prod.||Cor', 'Resultado']
        df_resultado['Resultado'] = df_resultado['Resultado'].astype(str)
        pedidos = pd.merge(pedidos, df_resultado, on='Pedido||Prod.||Cor', how='left')

        # 19.1: Atualizando a coluna 'Distribuicao' diretamente
        condicao = (pedidos['Resultado'] == 'False') & (
                (pedidos['Distribuicao'] == 'SIM') & (pedidos['Qtd Atende por Cor'] > 0))
        pedidos.loc[condicao, 'Distribuicao'] = 'SIM(Redistribuir)'
        # etapa21 = controle.salvarStatus_Etapa21(rotina, ip, etapa20,'Encontrando no pedido o percentual que atende a distribuicao')  # Registrar etapa no controlador

        # 20- Obtendo valor atente por cor
        pedidos['Valor Atende por Cor'] = pedidos['Qtd Atende por Cor'] * pedidos['PrecoLiquido']
        pedidos['Valor Atende por Cor'] = pedidos['Valor Atende por Cor'].astype(float).round(2)
        # etapa22 = controle.salvarStatus_Etapa22(rotina, ip, etapa21,'Obtendo valor atente por cor')  # Registrar etapa no controlador

        # 21 Identificando a Quantidade Distribuida
        # pedidos['Qnt. Cor(Distrib.)'] = pedidos.apply(lambda row: row['Qtd Atende por Cor'] if row['Distribuicao'] == 'SIM' else 0, axis=1)
        pedidos['Qnt. Cor(Distrib.)'] = pedidos['Qtd Atende por Cor'].where(pedidos['Distribuicao'] == 'SIM', 0)

        pedidos['Qnt. Cor(Distrib.)'] = pedidos['Qnt. Cor(Distrib.)'].astype(int)
        # etapa23 = controle.salvarStatus_Etapa23(rotina, ip, etapa22, 'Obtendo valor atente por cor')#Registrar etapa no controlador

        # 22 Obtendo valor atente por cor Distribuida
        # pedidos['Valor Atende por Cor(Distrib.)'] = pedidos.apply(lambda row: row['Valor Atende por Cor'] if row['Distribuicao'] == 'SIM' else 0, axis=1)
        pedidos['Valor Atende por Cor(Distrib.)'] = pedidos['Valor Atende por Cor'].where(
            pedidos['Distribuicao'] == 'SIM', 0)
        pedidos['Valor Atende'] = pedidos['Qtd Atende'] * pedidos['PrecoLiquido']
        pedidos['Valor Atende'] = pedidos['Valor Atende'].astype(float).round(2)
        pedidos.drop(['situacaobloq', 'dias_a_adicionar', 'Resultado'], axis=1, inplace=True)

        # etapa24 = controle.salvarStatus_Etapa24(rotina, ip, etapa23, 'Obtendo valor atente por cor Distribuida')#Registrar etapa no controlador
        pedidos['dataPrevAtualizada'] = pedidos['dataPrevAtualizada'].dt.strftime('%d/%m/%Y')
        pedidos["descricaoCondVenda"].fillna('-', inplace=True)
        pedidos["ultimo_fat"].fillna('-', inplace=True)
        pedidos["Status"].fillna('-', inplace=True)

        # Ciclo 2
        situacao = pedidos.groupby('codPedido')['Valor Atende por Cor(Distrib.)'].sum().reset_index()
        situacao = situacao[situacao['Valor Atende por Cor(Distrib.)'] > 0]
        situacao.columns = ['codPedido', 'totalPçDis']
        pedidos = pd.merge(pedidos, situacao, on='codPedido', how='left')
        pedidos.fillna(0, inplace=True)

        pedidos1 = pedidos[pedidos['totalPçDis'] == 0]
        pedidos1['SituacaoDistrib'] = 'Redistribui'
        pedidos1 = self.Ciclo2(pedidos1, avaliar_grupo)
        pedidos2 = pedidos[pedidos['totalPçDis'] > 0]
        pedidos2['SituacaoDistrib'] = 'Distribuido1'

        pedidos = pd.concat([pedidos1, pedidos2])

        # 23- Salvando os dados gerados em csv
        # retirar as seguintes colunas: StatusSugestao, situacaobloq, dias_a_adicionar, Resultado    monitor.fillna('', inplace=True)
        pedidos['codProduto'] = pedidos['codProduto'].astype(str)
        pedidos['codCor'] = pedidos['codCor'].astype(str)
        pedidos['nomeSKU'] = pedidos['nomeSKU'].astype(str)
        pedidos['Pedido||Prod.||Cor'] = pedidos['Pedido||Prod.||Cor'].astype(str)

        descricaoArquivo = self.dataInicioFat + '_' + self.dataFinalFat
        fp.write(f'./dados/monitor{descricaoArquivo}.parquet', pedidos)

        # etapa25 = controle.salvarStatus_Etapa25(rotina, ip, etapa24, 'Salvando os dados gerados no postgre')#Registrar etapa no controlador
        return pedidos

    def capaPedidos(self):
        '''Metodo que busca a capa dos pedidos no periodo de vendas'''
        tiponota = '1,2,3,4,5,6,7,8,10,24,92,201,1012,77,27,28,172,9998,66,67,233,237'  # Arrumar o Tipo de Nota 40
        empresa = "'" + str(self.empresa) + "'"

        if self.filtroDataEmissaoFim != '' and self.filtroDataEmissaoIni != '':
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

        else:
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

        if self.filtroDataEmissaoFim != '' and self.filtroDataEmissaoIni != '':
            sqlCswCapaPedidosDataPrev = """
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
                                                codEmpresa = """ + empresa + """
                                                and dataPrevFat >= '""" + self.dataInicioVendas + """' 
                                                and dataPrevFat <= '""" + self.dataFinalFat + """'
                                                and dataEmissao >= '""" +self.filtroDataEmissaoIni+"""'
                                                and dataEmissao <= '""" +self.filtroDataEmissaoFim+"""'  
                                                and codTipoNota in (""" + tiponota + """)  
                                        """
        else:
            sqlCswCapaPedidosDataPrev = """
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
                                        codEmpresa = """ + empresa + """
                                        and  dataPrevFat >= '""" + self.dataInicioVendas + """' 
                                        and dataPrevFat <= '""" + self.dataFinalFat + """' 
                                        and codTipoNota in (""" + tiponota + """)  """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                print(sqlCswCapaPedidosDataPrev)
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
                "nome" as "nomeSKU",
                categoria::varchar as "CATEGORIA"
            from 
                pcp."itens_csw"  
            where  
            	"codItemPai" not like '6%'
            """, conn)

        return consultar

    def Classificacao(self, pedidos, parametro):
        if parametro == 'Faturamento':
            # Define os valores de 'codSitSituacao' com base na condição para Faturamento
            pedidos.loc[
                (pedidos['codSitSituacao'] == '0') | (pedidos['codSitSituacao'] == '1'), 'codSitSituacao'] = '2-InicioFila'
            pedidos.loc[(pedidos['codSitSituacao'] != '2-InicioFila'), 'codSitSituacao'] = '1-FimFila'
            pedidos = pedidos.sort_values(by=['codSitSituacao', 'vlrSaldo'], ascending=False)
        elif parametro == 'DataPrevisao':
            # Define os valores de 'codSitSituacao' com base na condição para DataPrevisao
            pedidos.loc[
                (pedidos['codSitSituacao'] == '0') | (pedidos['codSitSituacao'] == '1'), 'codSitSituacao'] = '1-InicioFila'
            pedidos.loc[(pedidos['codSitSituacao'] != '1-InicioFila'), 'codSitSituacao'] = '2-FimFila'
            pedidos = pedidos.sort_values(by=['codSitSituacao', 'dataPrevAtualizada'], ascending=True)

        return pedidos

    def ConfiguracaoPercEntregas(self):
        conn = ConexaoPostgreWms.conexaoEngine()

        consultar = pd.read_sql(
            """Select * from pcp.monitor_fat_dados """, conn)

        consultar['Entregas Restantes'] = consultar['Entregas Restantes'].astype(str)

        return consultar

    def ConfiguracaoCategoria(self):
        conn = ConexaoPostgreWms.conexaoEngine()

        consultar = pd.read_sql(
            """
            Select "Opção" as "CATEGORIA", "Status" from pcp.monitor_check_status 
            """, conn)

        return consultar

    def Ciclo2(self,pedidos, avaliar_grupo):
        ###### O Ciclo2 e´usado para redistribuir as quantidades dos skus  que nao conseguiram atender na distribuicao dos pedidos no primeiro ciclo.
        # etapa 1: recarregando estoque

        estoque = EstoqueSkuClass.EstoqueSKU().consultaEstoqueConsolidadoPorReduzido_nat5()  # é feito uma nova releitura do estoque
        print('verificar se foi pra redistribuicao')

        pedidos1 = pedidos[pedidos['StatusSugestao'] == 'Nao Sugerido']
        pedidos2 = pedidos[pedidos['StatusSugestao'] != 'Nao Sugerido']

        pedidos1['codProduto'].fillna(0, inplace=True)
        pedidos1['codProduto'] = pedidos1['codProduto'].astype(str)

        SKUnovaReserva = pedidos1.groupby('codProduto').agg({'Qnt. Cor(Distrib.)': 'sum'}).reset_index()

        pedidos1.drop(['EstoqueLivre', 'estoqueAtual', 'estReservPedido',
                       'Necessidade', 'Qtd Atende', 'Saldo +Sugerido',
                       'Saldo Grade', 'X QTDE ATENDE', 'Qtd Atende por Cor', 'Fecha Acumulado',
                       'Saldo +Sugerido_Sum', '% Fecha Acumulado', '% Fecha pedido', 'Distribuicao',
                       'Valor Atende por Cor', 'Qnt. Cor(Distrib.)'
                          , 'Valor Atende por Cor(Distrib.)', 'Valor Atende', 'totalPçDis', 'SituacaoDistrib'], axis=1,
                      inplace=True)

        # 2.1 Somando todas as cores que conseguiu distriubuir no ciclo 1 para depois abater
        SKUnovaReserva.rename(columns={'Qnt. Cor(Distrib.)': 'ciclo1'}, inplace=True)
        estoque['codProduto'] = estoque['codProduto'].astype(str)
        estoque2 = pd.merge(estoque, SKUnovaReserva, on='codProduto', how='left')

        # Etapa3 filtrando somente os pedidos nao distibuidos e fazendo o merge com o estoque
        pedidos1 = pd.merge(pedidos1, estoque2, on='codProduto', how='left')
        pedidos1['EstoqueLivre'] = pedidos1['estoqueAtual'] - pedidos1['estReservPedido'] - pedidos1['ciclo1']

        # Etapa 4 - Calculando a Nova Necessidade e descobrindo o quanto atente por cor
        pedidos1['Necessidade'] = pedidos1.groupby('codProduto')['QtdSaldo'].cumsum()
        pedidos1['Qtd Atende'] = pedidos1['QtdSaldo'].where(pedidos1['Necessidade'] <= pedidos1['EstoqueLivre'], 0)
        pedidos1.loc[pedidos1['qtdeSugerida'] > 0, 'Qtd Atende'] = pedidos1['qtdeSugerida']
        pedidos1['Qtd Atende'] = pedidos1['Qtd Atende'].astype(int)
        pedidos1['Saldo +Sugerido'] = pedidos1['QtdSaldo'] + pedidos1['qtdeSugerida']
        pedidos1['Saldo Grade'] = pedidos1.groupby('Pedido||Prod.||Cor')['Saldo +Sugerido'].transform('sum')
        pedidos1['X QTDE ATENDE'] = pedidos1.groupby('Pedido||Prod.||Cor')['Qtd Atende'].transform('sum')
        pedidos1['Qtd Atende por Cor'] = pedidos1['Saldo +Sugerido'].where(
            pedidos1['Saldo Grade'] == pedidos1['X QTDE ATENDE'], 0)
        pedidos1['Qtd Atende por Cor'] = pedidos1['Qtd Atende por Cor'].astype(int)

        # Etapa 5: Encontrando o novo % Fecha Acumalado para o ciclo2
        pedidos1['Fecha Acumulado'] = pedidos1.groupby('codPedido')['Qtd Atende por Cor'].cumsum().round(2)
        pedidos1['Saldo +Sugerido_Sum'] = pedidos1.groupby('codPedido')['Saldo +Sugerido'].transform('sum')
        pedidos1['% Fecha Acumulado'] = (pedidos1['Fecha Acumulado'] / pedidos1['Saldo +Sugerido_Sum']).round(2) * 100
        pedidos1['% Fecha Acumulado'] = pedidos1['% Fecha Acumulado'].astype(str)
        pedidos1['% Fecha Acumulado'] = pedidos1['% Fecha Acumulado'].str.slice(0, 4)
        pedidos1['% Fecha Acumulado'] = pedidos1['% Fecha Acumulado'].astype(float)

        pedidos1['% Fecha pedido'] = (pedidos1.groupby('codPedido')['Qtd Atende por Cor'].transform('sum')) / (
            pedidos1.groupby('codPedido')['Saldo +Sugerido'].transform('sum'))
        pedidos1['% Fecha pedido'] = pedidos1['% Fecha pedido'] * 100
        pedidos1['% Fecha pedido'] = pedidos1['% Fecha pedido'].astype(float).round(2)

        # Etapa6: Obtendo novos valores para a distribuicao
        pedidos1['ValorMin'] = pedidos1['ValorMin'].astype(float)
        pedidos1['ValorMax'] = pedidos1['ValorMax'].astype(float)
        condicoes = [(pedidos1['% Fecha pedido'] >= pedidos1['ValorMin']) &
                     (pedidos1['% Fecha pedido'] <= pedidos1['ValorMax']),
                     (pedidos1['% Fecha pedido'] > pedidos1['ValorMax']) &
                     (pedidos1['% Fecha Acumulado'] <= pedidos1['ValorMax']),
                     (pedidos1['% Fecha pedido'] > pedidos1['ValorMax']) &
                     (pedidos1['% Fecha Acumulado'] > pedidos1['ValorMax']),
                     (pedidos1['% Fecha pedido'] < pedidos1['ValorMin'])
                     # adicionar mais condições aqui, se necessário
                     ]
        valores = ['SIM', 'SIM', 'SIM(Redistribuir)', 'NAO']  # definir os valores correspondentes
        pedidos1['Distribuicao'] = numpy.select(condicoes, valores, default=True)

        # Etapa 7: Avaliando se no nivel de pedido||sku||cor possui situacao de quebra
        df_resultado = pedidos1.loc[:, ['Pedido||Prod.||Cor', 'Distribuicao']]
        df_resultado = df_resultado.groupby('Pedido||Prod.||Cor')['Distribuicao'].apply(avaliar_grupo).reset_index()
        df_resultado.columns = ['Pedido||Prod.||Cor', 'Resultado']
        df_resultado['Resultado'] = df_resultado['Resultado'].astype(str)
        pedidos1 = pd.merge(pedidos1, df_resultado, on='Pedido||Prod.||Cor', how='left')
        # 7.1 Aplicando nova situacao no redistriuir
        condicao = (pedidos1['Resultado'] == 'False') & (
                (pedidos1['Distribuicao'] == 'SIM') & (pedidos1['Qtd Atende por Cor'] > 0))
        pedidos1.loc[condicao, 'Distribuicao'] = 'SIM(Redistribuir)'

        # 8- Encontradno os novos valores para o ciclo2:
        pedidos1['Valor Atende por Cor'] = pedidos1['Qtd Atende por Cor'] * pedidos1['PrecoLiquido']
        pedidos1['Valor Atende por Cor'] = pedidos1['Valor Atende por Cor'].astype(float).round(2)
        pedidos1['Qnt. Cor(Distrib.)'] = pedidos1['Qtd Atende por Cor'].where(pedidos1['Distribuicao'] == 'SIM', 0)
        pedidos1['Qnt. Cor(Distrib.)'] = pedidos1['Qnt. Cor(Distrib.)'].astype(int)
        pedidos1['Valor Atende por Cor(Distrib.)'] = pedidos1['Valor Atende por Cor'].where(
            pedidos1['Distribuicao'] == 'SIM', 0)
        pedidos1['Valor Atende'] = pedidos1['Qtd Atende'] * pedidos1['PrecoLiquido']
        pedidos1['Valor Atende'] = pedidos1['Valor Atende'].astype(float).round(2)

        situacao = pedidos1.groupby('codPedido')['Valor Atende por Cor(Distrib.)'].sum().reset_index()
        situacao = situacao[situacao['Valor Atende por Cor(Distrib.)'] > 0]
        situacao.columns = ['codPedido', 'totalPçDis']
        pedidos1 = pd.merge(pedidos1, situacao, on='codPedido', how='left')

        pedidos1['SituacaoDistrib'] = numpy.where(pedidos1['totalPçDis'] > 0, 'Distribuido2', 'Nao Redistribui')

        pedidosNovo = pd.concat([pedidos1, pedidos2])

        return pedidosNovo
    def monitorCsv(self):
        lerMonitor = ''
    def resumoMonitor(self):
        calcularMonitor = self.gerarMonitorPedidos()
        calcularMonitor['codPedido'] = calcularMonitor['codPedido'].astype(str)
        calcularMonitor['codCliente'] = calcularMonitor['codCliente'].astype(str)
        calcularMonitor["StatusSugestao"].fillna('-', inplace=True)
        calcularMonitor = calcularMonitor.groupby('codPedido').agg({
            "MARCA": 'first',
            "codTipoNota": 'first',
            "dataEmissao": 'first',
            "dataPrevFat": 'first',
            "dataPrevAtualizada": 'first',
            "codCliente": 'first',
            # "razao": 'first',
            "vlrSaldo": 'first',
            # "descricaoCondVenda": 'first',
            "entregas_Solicitadas": 'first',
            "entregas_enviadas": 'first',
            "qtdPecasFaturadas": 'first',
            'Saldo +Sugerido': 'sum',
            "ultimo_fat": "first",
            "Qtd Atende": 'sum',
            'QtdSaldo': 'sum',
            'Qtd Atende por Cor': 'sum',
            'Valor Atende por Cor': 'sum',
            # 'Valor Atende': 'sum',
            'StatusSugestao': 'first',
            'Valor Atende por Cor(Distrib.)': 'sum',
            'Qnt. Cor(Distrib.)': 'sum',
            'SituacaoDistrib': 'first'
            # 'observacao': 'first'
        }).reset_index()

        calcularMonitor['%'] = calcularMonitor['Qnt. Cor(Distrib.)'] / (calcularMonitor['Saldo +Sugerido'])
        calcularMonitor['%'] = calcularMonitor['%'] * 100
        calcularMonitor['%'] = calcularMonitor['%'].round(0)

        calcularMonitor.rename(columns={'MARCA': '01-MARCA', "codPedido": "02-Pedido",
                                "codTipoNota": "03-tipoNota", "dataPrevFat": "04-Prev.Original",
                                "dataPrevAtualizada": "05-Prev.Atualiz", "codCliente": "06-codCliente",
                                "vlrSaldo": "08-vlrSaldo", "entregas_Solicitadas": "09-Entregas Solic",
                                "entregas_enviadas": "10-Entregas Fat",
                                "ultimo_fat": "11-ultimo fat", "qtdPecasFaturadas": "12-qtdPecas Fat",
                                "Qtd Atende": "13-Qtd Atende", "QtdSaldo": "14- Qtd Saldo",
                                "Qnt. Cor(Distrib.)": "21-Qnt Cor(Distrib.)", "%": "23-% qtd cor",
                                "StatusSugestao": "18-Sugestao(Pedido)", "Qtd Atende por Cor": "15-Qtd Atende p/Cor",
                                "Valor Atende por Cor": "16-Valor Atende por Cor",
                                "Valor Atende por Cor(Distrib.)": "22-Valor Atende por Cor(Distrib.)"}, inplace=True)

        calcularMonitor = calcularMonitor.sort_values(by=['23-% qtd cor', '08-vlrSaldo'],
                                      ascending=False)  # escolher como deseja classificar
        calcularMonitor["10-Entregas Fat"].fillna(0, inplace=True)
        calcularMonitor["09-Entregas Solic"].fillna(0, inplace=True)

        calcularMonitor["11-ultimo fat"].fillna('-', inplace=True)
        calcularMonitor["05-Prev.Atualiz"].fillna('-', inplace=True)
        calcularMonitor.fillna(0, inplace=True)

        calcularMonitor["16-Valor Atende por Cor"] = calcularMonitor["16-Valor Atende por Cor"].round(2)
        calcularMonitor["22-Valor Atende por Cor(Distrib.)"] = calcularMonitor["22-Valor Atende por Cor(Distrib.)"].round(2)

        saldo = calcularMonitor['08-vlrSaldo'].sum()
        TotalQtdCor = calcularMonitor['15-Qtd Atende p/Cor'].sum()
        TotalValorCor = calcularMonitor['16-Valor Atende por Cor'].sum()
        TotalValorCor = TotalValorCor.round(2)

        totalPedidos = calcularMonitor['02-Pedido'].count()
        PedidosDistribui = calcularMonitor[calcularMonitor['23-% qtd cor'] > 0]
        PedidosDistribui = PedidosDistribui['02-Pedido'].count()

        pedidosRedistribuido = calcularMonitor[calcularMonitor['SituacaoDistrib'] == 'Distribuido2']
        pedidosRedistribuido = pedidosRedistribuido['SituacaoDistrib'].count()

        TotalQtdCordist = calcularMonitor['21-Qnt Cor(Distrib.)'].sum()
        TotalValorCordist = calcularMonitor['22-Valor Atende por Cor(Distrib.)'].sum()
        TotalValorCordist = TotalValorCordist.round(2)

        # Agrupando os clientes
        # Função para concatenar os valores agrupados
        def concat_values(group):
            return '/'.join(str(x) for x in group)

        # Agrupar e aplicar a função de concatenação
        result = calcularMonitor.groupby('06-codCliente')['02-Pedido'].apply(concat_values).reset_index()
        # Renomear as colunas
        result.columns = ['06-codCliente', 'Agrupamento']
        pedidos = pd.merge(calcularMonitor, result, on='06-codCliente', how='left')

        dados = {
            '0-Status': True,
            '1-Total Qtd Atende por Cor': f'{TotalQtdCor} Pçs',
            '2-Total Valor Valor Atende por Cor': f'{TotalValorCor}',
            '3-Total Qtd Cor(Distrib.)': f'{TotalQtdCordist} Pçs',
            '4-Total Valor Atende por Cor(Distrib.)': f'{TotalValorCordist}',
            '5-Valor Saldo Restante': f'{saldo}',
            '5.1-Total Pedidos': f'{totalPedidos}',
            '5.2-Total Pedidos distribui': f'{PedidosDistribui},({pedidosRedistribuido} pedidos redistribuido)',
            '6 -Detalhamento': pedidos.to_dict(orient='records')
        }
        return pd.DataFrame([dados])
    def gerarMonitorOps(self):
        '''Metodo utilizado para processar as prioridades a nivel de OP nos Pedidos
        return:
        DataFrame:
        '''
        # Definir o nome do arquivo:

        # Passo 1 : Carregar os dados da OP
        self.congelamentoOrdemProd()
        consulta = self.consultaSQLOrdemProd()

        # Carregar o arquivo Parquet com os parametros do monitor de pedidos, caso o usuario opte por filtrar o monitor, acessa o arquivo monitor_filtro
        try:
            parquet_file = fp.ParquetFile(f'./dados/monitor{self.descricaoArquivo}.parquet')
            # Converter para DataFrame do Pandas
            monitor = parquet_file.to_pandas()
            # disponibiliza um novo arquivo para ser utilizado com filtragem
            fp.write(f'./dados/monitor_filtro.parquet', monitor)
        except:
            parquet_file = fp.ParquetFile(f'./dados/monitor_filtro.parquet')
            monitor = parquet_file.to_pandas()


        # Condição para o cálculo da coluna 'NecessodadeOP'
        monitor['Qtd Atende'] = monitor['QtdSaldo'].where(monitor['Necessidade'] <= monitor['EstoqueLivre'], 0)
        condicao = (monitor['Qtd Atende'] > 0)
        # Cálculo da coluna 'NecessodadeOP' de forma vetorizada
        monitor['NecessodadeOP'] = numpy.where(condicao, 0, monitor['QtdSaldo'])
        monitor['NecessodadeOPAcum'] = monitor.groupby('codProduto')['NecessodadeOP'].cumsum()

        # Atribuindo valores padrao para identificar id_op e OP Reservada
        monitor['id_op'] = '-'
        monitor['Op Reservada'] = '-'

        for i in range(1,8):
            # filtra o dataframe consulta de acordo com a iteracao
            consultai = consulta[consulta['ocorrencia_sku'] == i]
            # Apresenta o numero de linhas do dataframe filtrado
            x = consultai['codProduto'].count()
            print(f'iteracao de classificacao {i}: {x}')
            # Realiza um merge com outro dataFrame Chamado Monitor
            monitor = pd.merge(monitor, consultai, on='codProduto', how='left')
            # Condição para o cálculo da coluna 'id_op'
            condicao = (monitor['NecessodadeOP'] == 0) | (monitor['NecessodadeOPAcum'] <= monitor['qtdAcumulada']) | (monitor['id_op'] == 'Atendeu')
            monitor['id_op'] = numpy.where(condicao, 'Atendeu', 'nao atendeu')
            monitor['Op Reservada'] = numpy.where(
                (monitor['id_op'] == 'Atendeu') &
                (monitor['NecessodadeOP'] > 0) &
                (monitor['Op Reservada'] == '-'),
                monitor['id'],  # Valor que será atribuído se a condição for verdadeira
                monitor['Op Reservada']  # Valor original se a condição for falsa
            )
            # Define NecessodadeOP para 0 onde id_op é 'Atendeu'
            monitor.loc[monitor['id_op'] == 'Atendeu', 'NecessodadeOP'] = 0
            monitor['NecessodadeOPAcum'] = monitor.groupby('codProduto')['NecessodadeOP'].cumsum()
            # Remove as colunas para depois fazer novo merge
            monitor = monitor.drop(['id', 'qtdAcumulada', 'ocorrencia_sku'], axis=1)


        # Estabelece no id_op o que AtendeuDistribuido pelo monitor original
        monitor.loc[monitor['Valor Atende por Cor(Distrib.)'] > 0, 'id_op'] = 'AtendeuDistribuido'
        # Estabelece o que ja foi faturado no id_op
        monitor.loc[monitor['QtdSaldo'] == 0, 'id_op'] = 'JaFaturada'

        # Condição para o cálculo da coluna 'Op Reservada'
        condicao = (monitor['Op Reservada'] == '-') & (monitor['id_op'] == 'Atendeu')
        # Atribui '9999||' onde a condição é verdadeira
        monitor.loc[condicao, 'Op Reservada'] = '0994||'

        # Condição para o cálculo da coluna 'Op Reservada'
        condicao = (monitor['Op Reservada'] == '-') & (monitor['id_op'] == 'AtendeuDistribuido')
        # Atribui '9999||' onde a condição é verdadeira
        monitor.loc[condicao, 'Op Reservada'] = '9997||'

        # Condição para o cálculo da coluna 'Op Reservada'
        condicao = (monitor['Op Reservada'] == '-') & (monitor['id_op'] == 'JaFaturada')
        # Atribui '9999||' onde a condição é verdadeira
        monitor.loc[condicao, 'Op Reservada'] = '9999||'

        def avaliar_grupo(df_grupo):
            return len(set(df_grupo)) == 1

        df_resultado = monitor.loc[:, ['Pedido||Prod.||Cor', 'id_op']]
        df_resultado = df_resultado.groupby('Pedido||Prod.||Cor')['id_op'].apply(avaliar_grupo).reset_index()
        df_resultado.columns = ['Pedido||Prod.||Cor', 'ResultadoAva']
        df_resultado['ResultadoAva'] = df_resultado['ResultadoAva'].astype(str)
        monitor = pd.merge(monitor, df_resultado, on='Pedido||Prod.||Cor', how='left')

        # Condição para o cálculo da coluna 'Op Reservada'
        condicao = (monitor['id_op'] == 'JaFaturada') & (monitor['ResultadoAva'] == 'False')
        monitor.loc[condicao, 'Op Reservada'] = '9998||'
        condicao = (monitor['ResultadoAva'] == 'False') & (monitor['id_op'] == 'AtendeuDistribuido')
        # Atribui '9999||' onde a condição é verdadeira
        monitor.loc[condicao, 'Op Reservada'] = '9996||'

        # Condição para o cálculo da coluna 'Op Reservada'
        condicao = (monitor['Op Reservada'] == '0994||') & (monitor['id_op'] == 'Atendeu') & (
                monitor['ResultadoAva'] == 'True')
        # Atribui '9999||' onde a condição é verdadeira
        monitor.loc[condicao, 'Op Reservada'] = '9995||'

        # Condição para o cálculo da coluna 'Op Reservada'
        condicao = (monitor['Op Reservada'].str.startswith('9')) & \
                   (monitor['Op Reservada'] != '0994||') & \
                   (monitor['id_op'] == 'Atendeu') & \
                   (monitor['ResultadoAva'] == 'False') & \
                   (monitor['Op Reservada'] != '9995||')

        # Aplicando a condição e substituindo o primeiro '9' por '0'
        monitor.loc[condicao, 'Op Reservada'] = monitor.loc[condicao, 'Op Reservada'].astype(str).str.replace(r'^9',
                                                                                                              '2',
                                                                                                              regex=True)

        monitor = monitor.sort_values(by=['codSitSituacao', 'dataPrevAtualizada', 'Op Reservada', 'Pedido||Prod.||Cor'],
                                      ascending=[True, True, False, True]).reset_index()

        monitor['recalculoData'] = monitor.groupby('codPedido')['Saldo Grade'].cumsum()
        monitor['recalculoData2'] = monitor.groupby('codPedido')['Saldo Grade'].transform('sum')
        monitor['recalculoData'] = (monitor['recalculoData'] / monitor['recalculoData2']).round(2)
        monitor['recalculoData'] = monitor['recalculoData'].astype(str)
        monitor['recalculoData'] = monitor['recalculoData'].str.slice(0, 4)
        monitor['recalculoData'] = monitor['recalculoData'].astype(float)
        # Certifique-se de que 'Entregas Restantes' é do tipo int
        monitor['Entregas Restantes'] = monitor['Entregas Restantes'].astype(int)

        # Condições para a coluna 'entregaAtualizada'
        cond_1 = (monitor['Entregas Restantes'] == 1)
        cond_2_1 = (monitor['Entregas Restantes'] == 2) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
        cond_2_2 = (monitor['Entregas Restantes'] == 2) & (monitor['recalculoData'] * 100 > monitor['ValorMax'])
        cond_3_1 = (monitor['Entregas Restantes'] == 3) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 1)
        cond_3_2 = (monitor['Entregas Restantes'] == 3) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
        cond_3_3 = (monitor['Entregas Restantes'] == 3) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2)
        cond_4_1 = (monitor['Entregas Restantes'] == 4) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
        cond_4_2 = (monitor['Entregas Restantes'] == 4) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
        cond_4_3 = (monitor['Entregas Restantes'] == 4) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
        cond_4_4 = (monitor['Entregas Restantes'] == 4) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3)

        monitor.loc[cond_1, 'entregaAtualizada'] = 1
        monitor.loc[cond_2_1, 'entregaAtualizada'] = 1
        monitor.loc[cond_2_2, 'entregaAtualizada'] = 2
        monitor.loc[cond_3_1, 'entregaAtualizada'] = 1
        monitor.loc[cond_3_2, 'entregaAtualizada'] = 2
        monitor.loc[cond_3_3, 'entregaAtualizada'] = 3
        monitor.loc[cond_4_1, 'entregaAtualizada'] = 1
        monitor.loc[cond_4_2, 'entregaAtualizada'] = 2
        monitor.loc[cond_4_3, 'entregaAtualizada'] = 3
        monitor.loc[cond_4_4, 'entregaAtualizada'] = 4

        # Caso 5: Entregas Restantes == 5
        cond_5_1 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
        cond_5_2 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
        cond_5_3 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
        cond_5_4 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 4)
        cond_5_5 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 4)
        monitor.loc[cond_5_1, 'entregaAtualizada'] = 1
        monitor.loc[cond_5_2, 'entregaAtualizada'] = 2
        monitor.loc[cond_5_3, 'entregaAtualizada'] = 3
        monitor.loc[cond_5_4, 'entregaAtualizada'] = 4
        monitor.loc[cond_5_5, 'entregaAtualizada'] = 5

        # Caso 6: Entregas Restantes == 6
        cond_6_1 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
        cond_6_2 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
        cond_6_3 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
        cond_6_4 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 4)
        cond_6_5 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 4) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 5)
        cond_6_6 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 5)
        monitor.loc[cond_6_1, 'entregaAtualizada'] = 1
        monitor.loc[cond_6_2, 'entregaAtualizada'] = 2
        monitor.loc[cond_6_3, 'entregaAtualizada'] = 3
        monitor.loc[cond_6_4, 'entregaAtualizada'] = 4
        monitor.loc[cond_6_5, 'entregaAtualizada'] = 5
        monitor.loc[cond_6_6, 'entregaAtualizada'] = 6

        # Caso 7: Entregas Restantes == 7
        cond_7_1 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
        cond_7_2 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
        cond_7_3 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
        cond_7_4 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 4)
        cond_7_5 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 4) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 5)
        cond_7_6 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 5) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 6)
        cond_7_7 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 6)
        monitor.loc[cond_7_1, 'entregaAtualizada'] = 1
        monitor.loc[cond_7_2, 'entregaAtualizada'] = 2
        monitor.loc[cond_7_3, 'entregaAtualizada'] = 3
        monitor.loc[cond_7_4, 'entregaAtualizada'] = 4
        monitor.loc[cond_7_5, 'entregaAtualizada'] = 5
        monitor.loc[cond_7_6, 'entregaAtualizada'] = 6
        monitor.loc[cond_7_7, 'entregaAtualizada'] = 7

        # Caso 8: Entregas Restantes == 8
        cond_8_1 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
        cond_8_2 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
        cond_8_3 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
        cond_8_4 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 4)
        cond_8_5 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 4) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 5)
        cond_8_6 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 5) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 6)
        cond_8_7 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 6) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 7)
        cond_8_8 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 7)
        monitor.loc[cond_8_1, 'entregaAtualizada'] = 1
        monitor.loc[cond_8_2, 'entregaAtualizada'] = 2
        monitor.loc[cond_8_3, 'entregaAtualizada'] = 3
        monitor.loc[cond_8_4, 'entregaAtualizada'] = 4
        monitor.loc[cond_8_5, 'entregaAtualizada'] = 5
        monitor.loc[cond_8_6, 'entregaAtualizada'] = 6
        monitor.loc[cond_8_7, 'entregaAtualizada'] = 7
        monitor.loc[cond_8_8, 'entregaAtualizada'] = 8

        monitor['dataPrevAtualizada2'] = (
                monitor['dataPrevAtualizada']
                .str.slice(6, 10) + '-' +
                monitor['dataPrevAtualizada'].str.slice(3, 5) + '-' +
                monitor['dataPrevAtualizada'].str.slice(0, 2)
        )

        # 7 Calculando a nova data de Previsao do pedido
        monitor['dias_a_adicionar2'] = pd.to_timedelta(((monitor['entregaAtualizada'] - 1) * 15),
                                                       unit='d')  # Converte a coluna de inteiros para timedelta
        monitor['dataPrevAtualizada2'] = pd.to_datetime(monitor['dataPrevAtualizada2'], errors='coerce',
                                                        infer_datetime_format=True)
        monitor['dataPrevAtualizada2'].fillna(self.dataInicioFat, inplace=True)

        monitor['dataPrevAtualizada2'] = monitor['dataPrevAtualizada2'] + monitor['dias_a_adicionar2']

        monitor = monitor.sort_values(by=['dataPrevAtualizada2'], ascending=True)

        monitor.drop(['NecessodadeOP', 'NecessodadeOPAcum', 'id_op', 'Op Reservada'], axis=1, inplace=True)

        # Condição para o cálculo da coluna 'NecessodadeOP'
        condicao = (monitor['Qtd Atende'] > 0)
        # Cálculo da coluna 'NecessodadeOP' de forma vetorizada
        monitor['NecessodadeOP'] = numpy.where(condicao, 0, monitor['QtdSaldo'])
        monitor['NecessodadeOPAcum'] = monitor.groupby('codProduto')['NecessodadeOP'].cumsum()

        monitor['id_op2'] = '-'
        monitor['Op Reservada2'] = '-'

        consulta2 = self.consultaSQLOrdemProd('qtdAcumulada2')

        for i in range(1, 8):
            consultai = consulta2[consulta2['ocorrencia_sku'] == i]
            x = consultai['codProduto'].count()
            print(f'iteracao final  {i}/8: {x}')

            monitor = pd.merge(monitor, consultai, on='codProduto', how='left', suffixes=('', '_consultai'))

            condicao = (monitor['NecessodadeOP'] == 0) | (monitor['NecessodadeOPAcum'] <= monitor['qtdAcumulada2']) | (
                    monitor['id_op2'] == 'Atendeu')
            monitor['id_op2'] = numpy.where(condicao, 'Atendeu', 'nao atendeu')

            monitor['Op Reservada2'] = numpy.where(
                (monitor['id_op2'] == 'Atendeu') & (monitor['NecessodadeOP'] > 0) & (monitor['Op Reservada2'] == '-'),
                monitor['id'].fillna(monitor['Op Reservada2']), monitor['Op Reservada2'])

            monitor.loc[monitor['id_op2'] == 'Atendeu', 'NecessodadeOP'] = 0
            monitor['NecessodadeOPAcum'] = monitor.groupby('codProduto')['NecessodadeOP'].cumsum()
            monitor = monitor.drop(['id', 'qtdAcumulada2', 'ocorrencia_sku'], axis=1)

        consulta3 = self.consultaIdOPReservada()
        monitor = pd.merge(monitor, consulta3, on='Op Reservada2', how='left')
        monitor.to_csv(f'./dados/monitorOps{self.descricaoArquivo}.csv')


        data = monitor[(monitor['dataPrevAtualizada2'] >= self.dataInicioFat) & (monitor['dataPrevAtualizada2'] <= self.dataFinalFat)]
        # Contar a quantidade de pedidos distintos para cada 'numeroop'
        unique_counts = data.drop_duplicates(subset=['numeroop', 'codPedido']).groupby('numeroop')['codPedido'].count()

        # Adicionar essa contagem ao DataFrame original
        monitor['Ocorrencia Pedidos'] = monitor['numeroop'].map(unique_counts)

        monitor1 = monitor[
            ['numeroop', 'dataPrevAtualizada2', 'codFaseAtual', "codItemPai", "QtdSaldo", "Ocorrencia Pedidos"]]
        monitor2 = monitor[['numeroop', 'dataPrevAtualizada2', 'codFaseAtual', "codItemPai", "QtdSaldo", "codProduto"]]

        # Converter a coluna 'dataPrevAtualizada2' para string no formato desejado
        monitor1['dataPrevAtualizada2'] = monitor1['dataPrevAtualizada2'].dt.strftime('%Y-%m-%d')

        monitor1['dataPrevAtualizada2'] = pd.to_datetime(monitor1['dataPrevAtualizada2'], errors='coerce',
                                                         infer_datetime_format=True)

        mascara = (monitor1['dataPrevAtualizada2'] >= self.dataInicioFat) & (monitor1['dataPrevAtualizada2'] <= self.dataFinalFat)
        monitor1['dataPrevAtualizada2'] = monitor1['dataPrevAtualizada2'].dt.strftime('%Y-%m-%d')

        monitor1['numeroop'].fillna('-', inplace=True)
        monitor1 = monitor1.loc[mascara]

        monitor1 = monitor1[monitor1['numeroop'] != '-']

        monitor1 = monitor1.groupby('numeroop').agg(
            {'codFaseAtual': 'first', 'Ocorrencia Pedidos': 'first', "codItemPai": "first",
             "QtdSaldo": "sum"}).reset_index()



        monitorDetalhadoOps = monitor2.groupby(['numeroop', 'codProduto']).agg({"QtdSaldo": "sum"}).reset_index()

        monitorDetalhadoOps.to_csv(f'./dados/detalhadoops{self.descricaoArquivo}.csv')

        monitor1 = monitor1.sort_values(by=['Ocorrencia Pedidos'],
                                        ascending=[False]).reset_index()
        monitor1.rename(columns={'QtdSaldo': 'AtendePçs'}, inplace=True)

        sqlCsw = """Select f.codFase as codFaseAtual , f.nome  from tcp.FasesProducao f WHERE f.codEmpresa = 1"""
        sqlCswPrioridade = """
            SELECT op.numeroOP as numeroop, p.descricao as prioridade, op.dataPrevisaoTermino, e.descricao,t.qtdOP, (select descricao from tcl.lote l where l.codempresa = 1 and l.codlote = op.codlote) as descricaoLote  FROM TCO.OrdemProd OP 
        left JOIN tcp.PrioridadeOP p on p.codPrioridadeOP = op.codPrioridadeOP and op.codEmpresa = p.Empresa 
        join tcp.engenharia e on e.codempresa = 1 and e.codEngenharia = op.codProduto
        left join (
        SELECT numeroop, sum(qtdePecas1Qualidade) as qtdOP FROM tco.OrdemProdTamanhos  
        where codempresa = 1 group by numeroop
        ) t on t.numeroop =op.numeroop
        WHERE op.situacao = 3 and op.codEmpresa = 1
            """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                get = pd.DataFrame(rows, columns=colunas)
                get['codFaseAtual'] = get['codFaseAtual'].astype(str)
                del rows

                cursor_csw.execute(sqlCswPrioridade)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                get2 = pd.DataFrame(rows, columns=colunas)
                del rows

        monitor1 = pd.merge(monitor1, get, on='codFaseAtual', how='left')
        monitor1 = pd.merge(monitor1, get2, on='numeroop', how='left')
        monitor1.fillna('-', inplace=True)

        dados = {
            '0-Status': True,
            '1-Mensagem': f'Atencao!! Calculado segundo o ultimo monitor emitido',
            '6 -Detalhamento': monitor1.to_dict(orient='records')

        }
        return pd.DataFrame([dados])
    def consultaIdOPReservada(self):

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta3 = pd.read_sql("""select id as "Op Reservada2" , numeroop, "codFaseAtual" from "pcp".ordemprod o   """,
                                conn)

        return consulta3
    def congelamentoOrdemProd(self):
        '''Metodo utilizado para congelar uma tabela no banco para a analise de acordo com o monitor escolhido'''

        diaAtual = self.obterDiaAtual()
        nome = self.descricaoArquivo+'_'+ diaAtual

        dropIf = """DROP TABLE IF EXISTS "PCP".pcp."ordemprod_monitor_"""+nome+""" ";"""

        sql = """
            create table 
                "PCP".pcp."ordemprod_monitor_"""+nome+""" "
                       as 
                       select 
                            *, 0::int as reservado
                    from "PCP".pcp.ordemprod o 
            """

        dataHoje_dt = datetime.strptime(self.obterDiaAtual(), "%Y-%m-%d")
        # Subtrair 3 dias
        dataAnterior3 = dataHoje_dt - timedelta(days=3)
        # Converter de volta para string no formato desejado
        dataAnterior3_str = dataAnterior3.strftime("%Y-%m-%d")
        print(dataAnterior3_str)

        sqlExclusao = """
        DO $$ 
        DECLARE
            r RECORD;
            data_limite DATE := '"""+dataAnterior3_str+"""' ;
        BEGIN
            FOR r IN 
                SELECT tablename 
                FROM pg_tables 
                WHERE schemaname = 'pcp'
                  AND tablename LIKE '%backup_%'
                  AND to_date(substring(tablename FROM 'backup_(\d{4}-\d{2}-\d{2})'), 'YYYY-MM-DD') < data_limite
            LOOP
                EXECUTE 'DROP TABLE IF EXISTS pcp.' || quote_ident(r.tablename);
            END LOOP;
        END $$;
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(dropIf)
                conn.commit()

                curr.execute(sql)
                conn.commit()

                curr.execute(sqlExclusao)
                conn.commit()
    def consultaSQLOrdemProd(self, apelodoColqtdAcumulado='qtdAcumulada' ):

        if apelodoColqtdAcumulado == "qtdAcumulada2":
            consultaSql = """
                select o.codreduzido as "codProduto", id, "qtdAcumulada" as "qtdAcumulada2", "ocorrencia_sku" from "pcp".ordemprod o where "qtdAcumulada" > 0
            """
        else:
            consultaSql = """
            select o.codreduzido as "codProduto", id, "qtdAcumulada", "ocorrencia_sku" from "pcp".ordemprod o where "qtdAcumulada" > 0
            """
        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consultaSql, conn)

        return  consulta
    def obterDiaAtual(self):


        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return agora

    def produtosSemOP_(self):

        # Ler o estoque atual a nivel de sku, retornando o merge com o monitor
        sql = """
                    select
            	        o.codreduzido::int AS "codProduto",
            	        sum(o.total_pcs) as total_pc
                    from
            	        "PCP".pcp.ordemprod o
                    where
            	        codreduzido::int > 0
                    group by
            	        codreduzido"""
        conn = ConexaoPostgreWms.conexaoEngine()
        sql = pd.read_sql(sql, conn)

        # 1 - ler o arquivo csv do monitor de ops
        descricaoArquivo = self.dataInicioFat + '_' + self.dataFinalFat
        monitorDetalhadoOps = pd.read_csv(f'./dados/monitorOps{descricaoArquivo}.csv')

        #monitorDetalhadoOps2 = monitorDetalhadoOps[
            #(monitorDetalhadoOps['id_op2'] == 'Atendeu') & (monitorDetalhadoOps['Op Reservada2'] != '-')].reset_index()


        monitorDetalhadoOps2 = monitorDetalhadoOps.groupby(['nomeSKU', 'codProduto']).agg(
            {'QtdSaldo': 'sum'}).reset_index()


        sql = pd.merge(sql, monitorDetalhadoOps2, on='codProduto', how='left')

        # Encontrando o saldo Naocomprometido do estoque
        sql['QtdSaldo'] = sql['total_pc'] - sql['QtdSaldo']
        sql.rename(columns={'QtdSaldo': 'QtdComprometido', 'total_pc': 'Total em OPs'}, inplace=True)
        sql = sql[sql['QtdComprometido'] > 0]
        sql = sql.drop(['codProduto'], axis=1)


        # Organizando a informacao para levandar o saldo sem op
        monitorDetalhadoOps = monitorDetalhadoOps[monitorDetalhadoOps['QtdSaldo'] > 0]
        monitorDetalhadoOps = monitorDetalhadoOps[monitorDetalhadoOps['id_op2'] == 'nao atendeu']
        monitorDetalhadoOps = monitorDetalhadoOps.groupby(['nomeSKU']).agg(
            {'QtdSaldo': 'sum', 'codItemPai': 'first', 'codProduto': 'first', 'codCor': 'first'}).reset_index()
        monitorDetalhadoOps.fillna('-', inplace=True)
        monitorDetalhadoOps = monitorDetalhadoOps[monitorDetalhadoOps['codItemPai'] != '-']
        monitorDetalhadoOps = monitorDetalhadoOps.sort_values(by=['QtdSaldo'],
                                                              ascending=[False]).reset_index()
        monitorDetalhadoOps.rename(columns={'codItemPai': 'codEngenharia'}, inplace=True)
        monitorDetalhadoOps['codEngenharia'] = monitorDetalhadoOps['codEngenharia'].astype(str)
        monitorDetalhadoOps = pd.merge(monitorDetalhadoOps, sql, on='nomeSKU', how='left')



        monitorDetalhadoOps.fillna(0, inplace=True)
        monitorDetalhadoOps['QtdSaldo'] = monitorDetalhadoOps['QtdSaldo'] - monitorDetalhadoOps['QtdComprometido']
        monitorDetalhadoOps = monitorDetalhadoOps[monitorDetalhadoOps['QtdSaldo'] > 0].reset_index()

        #Alternativa para transformar o valor
        #monitorDetalhadoOps['codEngenharia'] = (
         #       monitorDetalhadoOps['codEngenharia']
          #      .str[:-2]  # Remove os dois últimos caracteres ".0"
           #     .str.zfill(9)  # Preenche com zeros à esquerda até 9 caracteres
            #    + '-0'  # Adiciona o sufixo "-0"
        #)
        monitorDetalhadoOps['tamanho'] = monitorDetalhadoOps['nomeSKU'].apply(lambda x: x.split()[-2])
        monitorDetalhadoOps['codProduto'] = monitorDetalhadoOps['codProduto'].astype(str)
        monitorDetalhadoOps.rename(columns={'codProduto': 'codReduzido'}, inplace=True)

        return monitorDetalhadoOps


    def mP_PrincipalSemOP(self):
        sql = """
        SELECT
            c.CodComponente,
            c.codProduto,
            c.codAplicacao,
            c2.codSortimento ,
            c2.seqTamanho,
            c2.quantidade
        FROM
            tcp.ComponentesVariaveis c
        inner join tcp.CompVarSorGraTam c2 on
            c2.codEmpresa = 1
            and c2.codProduto = c.codProduto
            and c2.sequencia = c.codSequencia
        WHERE
            c.codEmpresa = 1
            and c.codAplicacao like '%PRINCI%'
            and c.codProduto like '%-0'
        """
