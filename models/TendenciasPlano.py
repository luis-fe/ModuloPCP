import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms
from models import Vendas, ProdutosClass, SimulacaoProg
from models.Meta import Meta
from dotenv import load_dotenv, dotenv_values
import os

class TendenciaPlano():
    '''Classe utilizada para a analise de tendencias de vendas de um determinado Plano '''

    def __init__(self, codPlano = None, parametroABC = None, perc_dist = None, empresa = '1',consideraPedBloq = 'nao', nomeSimulacao = ''):

        self.codPlano = codPlano
        self.parametroABC = parametroABC
        self.perc_dist = perc_dist
        self.nomeSimulacao = nomeSimulacao

        if self.perc_dist != None:
            if ',' in self.perc_dist:
                self.perc_dist = self.perc_dist.replace(',', '.')



        self.empresa = empresa
        self.consideraPedBloq = consideraPedBloq

    def consultaPlanejamentoABC(self):
        '''Metodo utilizado para planejar a distribuicacao ABC'''

        sql = """
        Select "nomeABC" , "perc_dist" from pcp."Plano_ABC"
        where 
            "codPlano" = %s
        order by 
            "nomeABC"
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        consulta['perc_Acumulado'] = consulta['perc_dist'].cumsum()


        totalDistribuido = consulta['perc_dist'].sum()
        faltaDistribuir = 100 - totalDistribuido

        data = {
                '1- Total Distribuido': f'{totalDistribuido}%',
                '2- Falta Distribuir':f'{faltaDistribuir}%',
                '3- Detalhamento:': consulta.to_dict(orient='records')
            }
        return pd.DataFrame([data])


    def inserirPlanejamentoABC(self):
        '''Metodo utilizado para inserir plano'''

        inserir = """
        insert into pcp."Plano_ABC" ("codPlano", "nomeABC", "perc_dist") values ( %s, %s, %s)
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(inserir,(self.codPlano, self.parametroABC,self.perc_dist))
                conn.commit()

        return pd.DataFrame([{'status':True,'Mensagem':'Planejamento ABC alterado com sucesso'}])

    def updatePlanejamentoABC(self):
        '''Metodo que faz o update no planejamento ABC do Plano'''

        update = """
        update pcp."Plano_ABC"
        set "perc_dist" = %s
        where "nomeABC" = %s and "codPlano" = %s
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(update,(self.perc_dist, self.parametroABC,self.codPlano))
                conn.commit()

        return pd.DataFrame([{'status':True,'Mensagem':'Planejamento ABC alterado com sucesso'}])

    def inserirOuAlterarPlanj_ABC(self):
        '''Metodo para inserir ou alterar o planejamento abc '''
        verifica1 = self.consultaParametrizacaoABC()
        verifica1 = verifica1[verifica1['nomeABC'] == self.parametroABC].reset_index()

        if verifica1.empty:
            return pd.DataFrame([{'status': False, 'Mensagem': 'Parametro Abc nao existe !'}])

        verifica = """
        Select "nomeABC" , "perc_dist" from pcp."Plano_ABC"
        where 
            "codPlano" = %s and "nomeABC" = %s
        """
        conn = ConexaoPostgreWms.conexaoEngine()
        verifica = pd.read_sql(verifica,conn,params=(self.codPlano, self.parametroABC,))
        if verifica.empty:
            self.inserirPlanejamentoABC()
        else:
            self.updatePlanejamentoABC()
        return pd.DataFrame([{'status':True,'Mensagem':'Inserido ou Atualizado com sucesso!'}])

    def consultaPlanABC(self):

        sql = """"
        select * from "Plano_ABC" 
        where "nomeABC" = %s and "codPlano" = %s
        """
        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.parametroABC, self.codPlano,))
        return consulta


    def arrayinserirOuAlterarPlanj_ABC(self, arrayABC , arrayPercentual):
        '''Metodo para inserir via abc '''





    def consultaParametrizacaoABC(self):
        '''Metodo utilizado para consultar a parametrizacao do ABC'''

        sql = """
        select "nomeABC" from pcp."parametroABC"
        order by "nomeABC" asc
        """
        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def inserirParametroABC(self):
        '''Metodo utilizado para cadastrar um novo paramentro ABC'''

        inserir = """
        insert into pcp."parametroABC" ("nomeABC") values (%s)
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(inserir,(self.parametroABC,))
                conn.commit()

        return pd.DataFrame([{'status':True,'Mensagem':'Novo parametroABC inserido com sucesso'}])

    def tendenciaVendas(self, aplicaTratamento = 'sim'):
        '''Metodo que desdobra a tendencia DE VENDAS  por sku '''

        # 1 - Carregando o acomponhamento de vendas:
        #########  instancia-se o objeto "vendas" da classe Vendas
        vendas = Vendas.VendasAcom(self.codPlano,self.empresa, self.consideraPedBloq)

            #### 1.2 listar as vendas por sku: retorna consultaVendasSku: tipo "dataFrame"
        consultaVendasSku = vendas.listagemPedidosSku()

            ##### 1.3 - desconsiderando as sacolas do tipo de nota de bonificacao
        mask = (consultaVendasSku['categoria'] == 'SACOLA') & (consultaVendasSku['codTipoNota'] == '40')
        consultaVendasSku = consultaVendasSku[~mask]

        # 2 - Agrupando as vendas por codProduto (codigo reduzido "sku")
        consultaVendasSku = consultaVendasSku.groupby(["codProduto"]).agg({"marca": "first",
                                                         "nome": 'first',
                                                         "categoria": 'first',
                                                         "codCor": "first",
                                                         "codItemPai": 'first',
                                                         "qtdePedida": "sum",
                                                         "qtdeFaturada": 'sum',
                                                         "valorVendido": 'sum',
                                                         "codSeqTamanho":'first',
                                                        "codSortimento":"first",
                                                         "codPedido": 'count'}).reset_index()
            # 2.1 - ordenando as vendas por sku
        consultaVendasSku = consultaVendasSku.sort_values(by=['qtdePedida'],
                                      ascending=False)
            # 2.2 - Filtrar os produtos diferente da categoria "Brindes"
        consultaVendasSku = consultaVendasSku[consultaVendasSku['categoria'] != 'BRINDES']

            #2.3 - pesquisando e alterando a sequencia de Tamanho pela descricao do tamanho
        tam = ProdutosClass.Produto().get_tamanhos()
        consultaVendasSku['codSeqTamanho'] = consultaVendasSku['codSeqTamanho'].astype(str).str.replace('.0', '', regex=False)
        consultaVendasSku = pd.merge(consultaVendasSku,tam,on='codSeqTamanho',how='left')


            # 2.4 Renomear as colunas necessárias
        consultaVendasSku.rename(columns={"codProduto": "codReduzido", "codPedido": "Ocorrencia em Pedidos"}, inplace=True)

            # 2.5 - Pesquisando e Acrescentando o status AFV "observancao: caso nao encontrado status de acomp ou bloqueio acrescenta como normal
        afv = ProdutosClass.Produto().statusAFV()
        consultaVendasSku.rename(columns={"codProduto":"codReduzido","codPedido":"Ocorrencia em Pedidos"}, inplace=True)
        consultaVendasSku = pd.merge(consultaVendasSku, afv, on='codReduzido',how='left')
        consultaVendasSku['statusAFV'].fillna('Normal',inplace=True)

        # 3 Filtrar categorias diferentes de 'sacola'
        consultaVendasSku['qtdePedida'] = np.where(
            (consultaVendasSku['codReduzido'] == '795532') | (consultaVendasSku['codReduzido'] == '795567'),
            consultaVendasSku['qtdePedida'] / 50,
            consultaVendasSku['qtdePedida']
        )

        consultaVendasSku['qtdeFaturada'] = np.where(
            (consultaVendasSku['codReduzido'] == '795532') | (consultaVendasSku['codReduzido'] == '795567'),
            consultaVendasSku['qtdeFaturada'] / 50,
            consultaVendasSku['qtdeFaturada']
        )

        # 4 - Obtendo a Meta por marca
        meta = Meta(self.codPlano).consultaMetaGeral()
        meta['metaPecas'] = meta['metaPecas'].str.replace('.','').astype(int)
        consultaVendasSku = pd.merge(consultaVendasSku,meta,on='marca',how='left')

        # 5 - Calculando o total de vendas e o que falta vender por MARCA
        consultaVendasSku['totalVendas'] = consultaVendasSku.groupby('marca')['qtdePedida'].transform('sum')


        # 6 Filtrar status AFV diferente de Bloqueado
        df_filtered1 = consultaVendasSku[consultaVendasSku['statusAFV']!='Bloqueado']

            # 6.1 Somar o acumulado de vendas por marca, considerando somente o status Normal
        vendas_acumuladasGeral = df_filtered1.groupby('marca')['qtdePedida'].sum()

            # 6.2 - Obtendo a distribuicao somente para os itens diferente de BLOQUEADO
        consultaVendasSku['vendasAcumuladasGeral'] = np.where(
            consultaVendasSku['statusAFV'] != 'Bloqueado',
            consultaVendasSku['marca'].map(vendas_acumuladasGeral),
            0
        )
        consultaVendasSku['distGeral%'] = np.where(
            consultaVendasSku['vendasAcumuladasGeral'] == 0,  # Condição
            0,
            consultaVendasSku['qtdePedida'] / consultaVendasSku['vendasAcumuladasGeral']  # Valor se falsa
        )

            # 6.3 Obtendo o falta a vender1 :
        consultaVendasSku['faltaVender1'] = consultaVendasSku['metaPecas'] - consultaVendasSku['totalVendas']
        consultaVendasSku['faltaVender1'] = consultaVendasSku['faltaVender1'].clip(lower=0)

            # 6.3 Obtendo a primeira previsao de vendas:
        consultaVendasSku['previcaoVendasGeral1'] = consultaVendasSku['distGeral%']* consultaVendasSku['faltaVender1']

                     # 6.4.1 - Consultando o Estoque da Natureza 5 e fazendo um "merge"  com ds dados
        estoque = ProdutosClass.Produto().estoqueNat5()
        consultaVendasSku = pd.merge(consultaVendasSku, estoque, on='codReduzido', how='left')
        consultaVendasSku['estoqueAtual'].fillna(0, inplace=True)

                    # 6.4.2 - Consultando as Ops em processo  e fazendo um "merge"  com ds dados
        emProcesso = ProdutosClass.Produto().emProducao()
        consultaVendasSku = pd.merge(consultaVendasSku, emProcesso, on='codReduzido', how='left')
        consultaVendasSku['emProcesso'].fillna(0, inplace=True)

                    # 6.4.3 Obtendo o disponivel dos produtos em acompanhamento:
        consultaVendasSku['disponivelAplica'] = (consultaVendasSku['estoqueAtual']+ consultaVendasSku['emProcesso'])-(consultaVendasSku['qtdePedida']- consultaVendasSku['qtdeFaturada'])

        consultaVendasSku['disponivelAcomp'] = np.where(
            (consultaVendasSku['statusAFV'] == 'Acompanhamento')&(consultaVendasSku['disponivelAplica'] > 0),
        consultaVendasSku['disponivelAplica']
            ,
            0
        )

            # 6.5 Decidindo qual a previsao dos produtos de acompanhamento
        consultaVendasSku['previcaoVendasGeral'] = np.where(
            (consultaVendasSku['disponivelAcomp'] < consultaVendasSku['previcaoVendasGeral1']) &
            (consultaVendasSku['statusAFV'] == 'Acompanhamento'),
            consultaVendasSku['disponivelAcomp'],
            consultaVendasSku['previcaoVendasGeral1']
        )

        # 7 - Redistribuindo para o Status AFV Normal o excedente da previsao
        consultaVendasSku['Redistribuir1'] = consultaVendasSku.groupby('marca')['previcaoVendasGeral'].transform('sum')
        consultaVendasSku['Redistribuir1_'] = consultaVendasSku['faltaVender1'] - consultaVendasSku['Redistribuir1']


            # 7.1 Filtrar status AFV Normal apenas, para podermos fazer uma redistribuicao
        df_filtered= consultaVendasSku[consultaVendasSku['statusAFV']=='Normal']

            # 7.2 Somar o acumulado de previcaoVendasGeral por marca, considerando somente o status Normal
        vendas_acumuladas = df_filtered.groupby('marca')['previcaoVendasGeral'].sum()



            # 7.3 - Considerar as vendas acumuladas somente para o status AFV normal
        consultaVendasSku['vendasAcumuladas'] = np.where(
            consultaVendasSku['statusAFV'] == 'Normal',
            consultaVendasSku['marca'].map(vendas_acumuladas),
            0
        )


            # 7.4 - Calculando o % distribuido para os status AFV IGUAL a Normal
        consultaVendasSku['dist%'] = np.where(
            consultaVendasSku['vendasAcumuladas'] == 0,  # Condição
            0,
            consultaVendasSku['previcaoVendasGeral'] / consultaVendasSku['vendasAcumuladas']  # Valor se falsa
        )

        # 8 - Encontradno a previsao de vendas
        consultaVendasSku['previcaoVendas'] = consultaVendasSku['dist%']* consultaVendasSku['Redistribuir1_']

        consultaVendasSku['dist%'] = consultaVendasSku['dist%'] *100
        consultaVendasSku['dist%'] = consultaVendasSku['dist%'].round(5)

        consultaVendasSku['previcaoVendas'].fillna(0,inplace=True)
        consultaVendasSku['previcaoVendas'] = consultaVendasSku['previcaoVendas'].round().astype(int)
        consultaVendasSku['previcaoVendasGeral'] = consultaVendasSku['previcaoVendasGeral'].round().astype(int)

        consultaVendasSku['previcaoVendas'] = consultaVendasSku['previcaoVendas'] + consultaVendasSku['previcaoVendasGeral']
        consultaVendasSku['previcaoVendas'] = consultaVendasSku['previcaoVendas'] + consultaVendasSku['qtdePedida']

        # 9.1 - Tratar os erros de NaN para retornar "0"
        consultaVendasSku.fillna(0,inplace=True)


        #if aplicaTratamento == 'sim':
        consultaVendasSku['subtotal'] = consultaVendasSku.groupby('marca')['previcaoVendas'].transform('sum')
        consultaVendasSku['redistribuir2'] = consultaVendasSku['metaPecas'] - consultaVendasSku['subtotal']


        # Filtrar as 200 primeiras linhas
        consultaVendasSku_200 = consultaVendasSku.iloc[:400]

        # Filtrar as linhas com status igual a 'NORMAL'
        filtro_normal = consultaVendasSku_200['statusAFV'] == 'Normal'

        # Calcular o subtotal apenas para as linhas filtradas
        consultaVendasSku.loc[:399, 'subtotal2'] = (
            consultaVendasSku_200.loc[filtro_normal]
            .groupby('marca')['previcaoVendas']
            .transform('sum')
        )
        consultaVendasSku['subtotal2'] = consultaVendasSku['previcaoVendas']/consultaVendasSku['subtotal2']

        consultaVendasSku['subtotal2'] = consultaVendasSku['subtotal2'] * consultaVendasSku['redistribuir2']
        consultaVendasSku['subtotal2'].fillna(0,inplace=True)

        consultaVendasSku['subtotal2'] = consultaVendasSku['subtotal2'].round().astype(int)
        consultaVendasSku['previcaoVendas'] = consultaVendasSku['previcaoVendas'] + consultaVendasSku['subtotal2']

        consultaVendasSku['subtotalPrev'] = consultaVendasSku.groupby('marca')['previcaoVendas'].transform('sum')
        consultaVendasSku['redistribuirPrev'] = consultaVendasSku['metaPecas'] - consultaVendasSku['subtotalPrev']




        '''
        consultaVendasSku['redistribuir'] = np.where(
            consultaVendasSku['redistribuir'] < 0 ,
            0,
            consultaVendasSku['redistribuir']
        )
        '''
        # 9.2 - Drop das colunas que nao desejo
        consultaVendasSku.drop(['faltaVender1','totalVendas','vendasAcumuladas','metaPecas','metaFinanceira'], axis=1, inplace=True)





        # 10 - Calculando o disponivel - baseado na quantidade pedida

        consultaVendasSku['disponivel'] = (consultaVendasSku['emProcesso'] + consultaVendasSku['estoqueAtual']) - (
                consultaVendasSku['qtdePedida'] - consultaVendasSku['qtdeFaturada'])

        # 11 - Calculando a Previsao de sobra  - baseado na previsao de vendas
        consultaVendasSku['Prev Sobra'] = (consultaVendasSku['emProcesso'] + consultaVendasSku['estoqueAtual']) - (
                consultaVendasSku['previcaoVendas'] - consultaVendasSku['qtdeFaturada'])

        # 12 - Calculando o falta programar, baseado na previsao de vendas
        consultaVendasSku['faltaProg (Tendencia)'] = consultaVendasSku['Prev Sobra'].where(consultaVendasSku['Prev Sobra'] < 0, 0)


        # 14 - Salvando o dataFrame na memoria do servidor, para ser congelado para analises
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        consultaVendasSku.to_csv(f'{caminhoAbsoluto}/dados/tenendicaPlano-{self.codPlano}.csv')

        # 15 - Tratando o valor financeiro
        consultaVendasSku['valorVendido'] = consultaVendasSku['valorVendido'].apply(self.formatar_financeiro)

        # 16 - Acescentando tendencia abc
        abc = self.tendenciaAbc('sim')
        abc['codItemPai'] = abc['codItemPai'].astype(str)
        consultaVendasSku = pd.merge(consultaVendasSku, abc , on='codItemPai', how='left')
        consultaVendasSku.fillna('-', inplace=True)




        return consultaVendasSku

    def tendenciaAbc(self, utilizaCongelento = 'nao'):
        '''Metodo que retorna a tendencia ABC '''

        # 1 - Verifica se é para utilizar ou nao o congelamento ABC que tem como premisa agilar a consulta quanto utilizado na tendenciaSku
        if utilizaCongelento == 'nao':
            vendas = Vendas.VendasAcom(self.codPlano, self.empresa, self.consideraPedBloq)
            consultaVendasSku = vendas.listagemPedidosSku()
            consultaVendasSku = consultaVendasSku[consultaVendasSku['categoria'] != 'SACOLA']

        else:
            load_dotenv('db.env')
            caminhoAbsoluto = os.getenv('CAMINHO')
            consultaVendasSku = pd.read_csv(f'{caminhoAbsoluto}/dados/tenendicaPlano-{self.codPlano}.csv')

        consultaVendasSku = consultaVendasSku.groupby(["codItemPai"]).agg({"marca": "first",
                                                                           "nome": 'first',
                                                                           "categoria": 'first',
                                                                           "qtdePedida": "sum",
                                                                           "qtdeFaturada": 'sum',
                                                                           "valorVendido": 'sum'}).reset_index()
        consultaVendasSku = consultaVendasSku.sort_values(by=['qtdePedida'],
                                                          ascending=False)  # escolher como deseja classificar


        consultaVendasSku['totalVendas'] = consultaVendasSku.groupby('marca')['qtdePedida'].transform('sum')
        consultaVendasSku['totalVendasCategoria'] = consultaVendasSku.groupby(['marca','categoria'])['qtdePedida'].transform('sum')


        consultaVendasSku['ABCdist%'] = np.where(
            consultaVendasSku['qtdePedida'] == 0,  # Condição
            0,  # Valor se condição for verdadeira
            consultaVendasSku['qtdePedida'] / consultaVendasSku['totalVendas']  # Valor se falsa
        )

        consultaVendasSku['ABCdist%Categoria'] = np.where(
            consultaVendasSku['qtdePedida'] == 0,  # Condição
            0,  # Valor se condição for verdadeira
            consultaVendasSku['qtdePedida'] / consultaVendasSku['totalVendasCategoria']  # Valor se falsa
        )


        consultaVendasSku['nome'] = consultaVendasSku['nome'].str.rsplit(' ', n=2).str[:-1].str.join(' ')
        consultaVendasSku['nome'] = consultaVendasSku['nome'].str.rsplit(' ', n=2).str[:-1].str.join(' ')
        consultaVendasSku['ABC_Acum%'] = consultaVendasSku.groupby('marca')['ABCdist%'].cumsum()
        consultaVendasSku['ABC_Acum%Categoria'] = consultaVendasSku.groupby(['marca','categoria'])['ABCdist%Categoria'].cumsum()


        consultaVendasSku['ABC_Acum%'] = consultaVendasSku['ABC_Acum%'].round(4)
        consultaVendasSku['ABC_Acum%Categoria'] = consultaVendasSku['ABC_Acum%Categoria'].round(4)


        # Consultando o ABC cadastrado para o Plano:
        sql = """
        Select "nomeABC" , "perc_dist", "codPlano" from pcp."Plano_ABC"
        where 
            "codPlano" = %s
        order by 
            "nomeABC"
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        consulta['perc_dist'] = consulta['perc_dist'].cumsum()
        consulta['perc_dist'] = consulta['perc_dist']/100
        # Adiciona os limites inferiores das faixas
        bins = [0] + consulta['perc_dist'].tolist()  # [0, 20, 50, 100]
        labels = consulta['nomeABC'].tolist()  # ['a', 'b', 'c']

        # Classifica cada percentual de vendas nas faixas definidas
        consultaVendasSku['class'] = pd.cut(
            consultaVendasSku['ABC_Acum%'],
            bins=bins,
            labels=labels,
            include_lowest=True
        )

        consultaVendasSku['classCategoria'] = pd.cut(
            consultaVendasSku['ABC_Acum%Categoria'],
            bins=bins,
            labels=labels,
            include_lowest=True
        )

        consultaVendasSku['class'] = consultaVendasSku['class'].astype(str)
        consultaVendasSku['class'].fillna('-', inplace=True)

        consultaVendasSku['classCategoria'] = consultaVendasSku['classCategoria'].astype(str)
        consultaVendasSku['classCategoria'].fillna('-', inplace=True)

        consultaVendasSku['ocorrencia'] = consultaVendasSku.groupby(['marca', 'categoria']).cumcount() + 1
        consultaVendasSku.loc[consultaVendasSku['ocorrencia'] == 1, 'classCategoria'] = 'A1'

        if utilizaCongelento == 'sim':
            consultaVendasSku = consultaVendasSku.loc[:,
                  ['codItemPai', 'class','classCategoria']]

        else:
            consultaVendasSku.drop(['ABCdist%', 'ABCdist%Categoria', "totalVendas", "totalVendasCategoria"], axis=1,
                                   inplace=True)

        return consultaVendasSku


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


    def simulacaoProgramacao(self, arraySimulaAbc= ''):
        '''Metodo utilizado para realizar a simulacao de programacao em nivel abc'''

        if arraySimulaAbc != '':
            r = self.simulacaoPeloArrayAbc(arraySimulaAbc)
            return r
        else:
            r = self.simulacaoPeloNome()
            return r



    def simulacaoPeloArrayAbc(self, arraySimulaAbc):

        # 1 - transformacao do array abc em DataFrame
        dfSimulaAbc = pd.DataFrame({
            'class': arraySimulaAbc[0],
            'percentual': arraySimulaAbc[1]
        })

        #2 - Caregar a tendencia congelada
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        tendencia = pd.read_csv(f'{caminhoAbsoluto}/dados/tenendicaPlano-{self.codPlano}.csv')


        abc = self.tendenciaAbc('sim')
        abc['codItemPai'] = abc['codItemPai'].astype(str)
        tendencia['codItemPai'] = tendencia['codItemPai'].astype(str)

        tendencia = pd.merge(tendencia,abc,on="codItemPai",how='left')
        tendencia = pd.merge(tendencia, dfSimulaAbc, on='class',how='left')

        tendencia['percentual'].fillna(0, inplace=True)

        tendencia['previcaoVendas'] = tendencia['previcaoVendas'] * (tendencia['percentual']/100)
        tendencia['previcaoVendas'] = tendencia['previcaoVendas'].round().astype(int)

        tendencia['Prev Sobra'] = (tendencia['emProcesso'] + tendencia['estoqueAtual']) - (
                tendencia['previcaoVendas'] - tendencia['qtdeFaturada'])
        tendencia['faltaProg (Tendencia)'] = tendencia['Prev Sobra'].where(
            tendencia['Prev Sobra'] < 0, 0)


        # 15 - Tratando o valor financeiro
        tendencia['valorVendido'] = tendencia['valorVendido'].apply(self.formatar_financeiro)



        return tendencia

    def simulacaoPeloNome(self):

        # 1 - transformacao do array abc em DataFrame
        dfSimulaAbc = SimulacaoProg.SimulacaoProg(self.nomeSimulacao).consultaSimulacaoAbc_s()
        dfSimulaCategoria = SimulacaoProg.SimulacaoProg(self.nomeSimulacao).consultaSimulacaoCategoria_s()
        dfSimulaMarca = SimulacaoProg.SimulacaoProg(self.nomeSimulacao).consultaSimulacaoMarca_s()

        # 2 - Caregar a tendencia congelada
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        tendencia = pd.read_csv(f'{caminhoAbsoluto}/dados/tenendicaPlano-{self.codPlano}.csv')

        tendencia['previcaoVendas'] = tendencia['previcaoVendas'] -tendencia['qtdePedida']

        abc = self.tendenciaAbc('sim')
        abc['codItemPai'] = abc['codItemPai'].astype(str)
        tendencia['codItemPai'] = tendencia['codItemPai'].astype(str)

        tendencia = pd.merge(tendencia, abc, on="codItemPai", how='left')
        tendencia = pd.merge(tendencia, dfSimulaAbc, on='class', how='left')
        tendencia['nomeSimulacao'] = self.nomeSimulacao

        tendencia['percentualABC'].fillna(100, inplace=True)

        tendencia = pd.merge(tendencia, dfSimulaCategoria, on='categoria', how='left')
        tendencia['percentualCategoria'].fillna(100, inplace=True)


        tendencia = pd.merge(tendencia, dfSimulaMarca, on='marca', how='left')
        tendencia['percentualMarca'].fillna(100, inplace=True)





        tendencia["percentual"] = tendencia[["percentualABC", "percentualCategoria", "percentualMarca"]].min(axis=1)

        tendencia['previcaoVendas'] = tendencia['previcaoVendas'] * (tendencia['percentual'] / 100)
        tendencia['previcaoVendas'] = tendencia['previcaoVendas'].round().astype(int)






        tendencia['previcaoVendas'] = tendencia['previcaoVendas'] + tendencia['qtdePedida']




        tendencia['Prev Sobra'] = (tendencia['emProcesso'] + tendencia['estoqueAtual']) - (
                tendencia['previcaoVendas'] - tendencia['qtdeFaturada'])
        tendencia['faltaProg (Tendencia)'] = tendencia['Prev Sobra'].where(
            tendencia['Prev Sobra'] < 0, 0)

        # 15 - Tratando o valor financeiro
        tendencia['valorVendido'] = tendencia['valorVendido'].apply(self.formatar_financeiro)

        return tendencia
