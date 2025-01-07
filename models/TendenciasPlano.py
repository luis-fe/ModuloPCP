import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms
from models import Vendas, ProdutosClass
from models.Meta import Meta


class TendenciaPlano():
    '''Classe utilizada para a analise de tendencias de vendas de um determinado Plano '''

    def __init__(self, codPlano = None, parametroABC = None, perc_dist = None, empresa = '1',consideraPedBloq = 'nao'):

        self.codPlano = codPlano
        self.parametroABC = parametroABC
        self.perc_dist = perc_dist

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
        '''Metodo que desdobra a tendencia ABC de vendas '''

        vendas = Vendas.VendasAcom(self.codPlano,self.empresa, self.consideraPedBloq)
        consultaVendasSku = vendas.listagemPedidosSku()
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
        consultaVendasSku = consultaVendasSku.sort_values(by=['qtdePedida'],
                                      ascending=False)  # escolher como deseja classificar
        tam = ProdutosClass.Produto().get_tamanhos()
        consultaVendasSku['codSeqTamanho'] = consultaVendasSku['codSeqTamanho'].astype(str).str.replace('.0','')
        tam['codSeqTamanho'] = tam['codSeqTamanho'].astype(str).str.replace('.0','')
        consultaVendasSku = pd.merge(consultaVendasSku,tam,on='codSeqTamanho',how='left')

        # Renomear colunas, se necessário
        consultaVendasSku.rename(columns={"codProduto": "codReduzido", "codPedido": "Ocorrencia em Pedidos"}, inplace=True)

        afv = ProdutosClass.Produto().statusAFV()
        consultaVendasSku.rename(columns={"codProduto":"codReduzido","codPedido":"Ocorrencia em Pedidos"}, inplace=True)
        consultaVendasSku = pd.merge(consultaVendasSku, afv, on='codReduzido',how='left')
        consultaVendasSku['statusAFV'].fillna('Normal',inplace=True)

        # Filtrar categorias diferentes de 'sacola'
        df_filtered = consultaVendasSku[consultaVendasSku['categoria'] != 'SACOLA']
        df_filtered= df_filtered[df_filtered['statusAFV']=='Normal']

        # Somar o acumulado de vendas por marca
        vendas_acumuladas = df_filtered.groupby('marca')['qtdePedida'].sum()

        # Mapear os valores acumulados para o DataFrame original
        consultaVendasSku['vendasAcumuladas'] = np.where(
            consultaVendasSku['categoria'] != 'Sacola',
            consultaVendasSku['marca'].map(vendas_acumuladas),
            0
        )

        # Mapear os valores acumulados para o DataFrame original
        consultaVendasSku['vendasAcumuladas'] = np.where(
            consultaVendasSku['statusAFV'] == 'Normal',
            consultaVendasSku['vendasAcumuladas'],
            0
        )

        consultaVendasSku['dist%'] = np.where(
            consultaVendasSku['vendasAcumuladas'] == 0,  # Condição
            0,  # Valor se condição for verdadeira
            consultaVendasSku['qtdePedida'] / consultaVendasSku['vendasAcumuladas']  # Valor se falsa
        )
        consultaVendasSku = consultaVendasSku[consultaVendasSku['categoria'] != 'SACOLA'].reset_index()
        #consultaVendasSku['%'] = consultaVendasSku.groupby('marca')['vendasAcumuladas'].cumsum()

        # Obtendo a Meta por marca
        meta = Meta(self.codPlano).consultaMetaGeral()
        meta['metaPecas'] = meta['metaPecas'].str.replace('.','').astype(int)

        consultaVendasSku = pd.merge(consultaVendasSku,meta,on='marca',how='left')
        consultaVendasSku['totalVendas'] = consultaVendasSku.groupby('marca')['qtdePedida'].transform('sum')

        consultaVendasSku['faltaVender'] = consultaVendasSku['metaPecas'] - consultaVendasSku['totalVendas']
        consultaVendasSku['faltaVender'] = consultaVendasSku['faltaVender'].clip(lower=0)

        consultaVendasSku['previcaoVendas'] = consultaVendasSku['dist%']* consultaVendasSku['faltaVender']
        consultaVendasSku['dist%'] = consultaVendasSku['dist%'].round(4)
        consultaVendasSku['dist%'] = consultaVendasSku['dist%'] *100
        consultaVendasSku['previcaoVendas'].fillna(0,inplace=True)
        consultaVendasSku['previcaoVendas'] = consultaVendasSku['previcaoVendas'].astype(int)
        consultaVendasSku['previcaoVendas'] = consultaVendasSku['previcaoVendas'] + consultaVendasSku['qtdePedida']

        '''
        #########################################################################################
        Verificar: 
        ['totalVendas'] considerear as vendas em geral indenpendente do status;
        ['faltaVender'] subtrair a meta geral pelo vendido ate o momento, caso negativo, considerar "0"
        bloqueio e acompanhamento nao deve ter previsao e nem % de distribuicao
        
        apos as operacoes a previsao deve ser tratata como int 
        #########################################################################################
        '''
        consultaVendasSku.fillna(0,inplace=True)
        consultaVendasSku.drop(['faltaVender','index','totalVendas','vendasAcumuladas','metaPecas','metaFinanceira'], axis=1, inplace=True)
        estoque = ProdutosClass.Produto().estoqueNat5()
        emProcesso = ProdutosClass.Produto().emProducao()
        consultaVendasSku = pd.merge(consultaVendasSku, estoque, on='codReduzido', how='left')
        consultaVendasSku['estoqueAtual'].fillna(0, inplace=True)
        consultaVendasSku = pd.merge(consultaVendasSku, emProcesso, on='codReduzido', how='left')
        consultaVendasSku['emProcesso'].fillna(0, inplace=True)
        consultaVendasSku['disponivel'] = (consultaVendasSku['emProcesso'] + consultaVendasSku['estoqueAtual']) - (
                consultaVendasSku['qtdePedida'] - consultaVendasSku['qtdeFaturada'])
        consultaVendasSku['Prev Sobra'] = (consultaVendasSku['emProcesso'] + consultaVendasSku['estoqueAtual']) - (
                consultaVendasSku['previcaoVendas'] - consultaVendasSku['qtdeFaturada'])
        consultaVendasSku['faltaProg (Tendencia)'] = consultaVendasSku['Prev Sobra'].where(consultaVendasSku['Prev Sobra'] < 0, 0)
        consultaVendasSku['valorVendido'] = consultaVendasSku['valorVendido'].apply(self.formatar_financeiro)

        if aplicaTratamento == 'sim':
            consultaVendasSku['estoqueAtual'] = consultaVendasSku['estoqueAtual'].apply(self.formatar_padraoInteiro)
            consultaVendasSku['emProcesso'] = consultaVendasSku['emProcesso'].apply(self.formatar_padraoInteiro)
            consultaVendasSku['qtdeFaturada'] = consultaVendasSku['qtdeFaturada'].apply(self.formatar_padraoInteiro)
            consultaVendasSku['qtdePedida'] = consultaVendasSku['qtdePedida'].apply(self.formatar_padraoInteiro)
            consultaVendasSku['previcaoVendas'] = consultaVendasSku['previcaoVendas'].apply(self.formatar_padraoInteiro)
            consultaVendasSku['disponivel'] = consultaVendasSku['disponivel'].apply(self.formatar_padraoInteiro)
            consultaVendasSku['faltaProg (Tendencia)'] = consultaVendasSku['faltaProg (Tendencia)'].apply(self.formatar_padraoInteiro)
            consultaVendasSku['Prev Sobra'] = consultaVendasSku['Prev Sobra'].apply(self.formatar_padraoInteiro)




        return consultaVendasSku

    def tendenciaAbc(self):
        '''Metodo que retorna a tendencia ABC '''

        vendas = Vendas.VendasAcom(self.codPlano, self.empresa, self.consideraPedBloq)

        consultaVendasSku = vendas.listagemPedidosSku()

        consultaVendasSku = consultaVendasSku.groupby(["codItemPai"]).agg({"marca": "first",
                                                                           "nome": 'first',
                                                                           "categoria": 'first',
                                                                           "qtdePedida": "sum",
                                                                           "qtdeFaturada": 'sum',
                                                                           "valorVendido": 'sum'}).reset_index()
        consultaVendasSku = consultaVendasSku.sort_values(by=['qtdePedida'],
                                                          ascending=False)  # escolher como deseja classificar
        consultaVendasSku = consultaVendasSku[consultaVendasSku['categoria'] != 'SACOLA'].reset_index()


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


        consultaVendasSku.drop(['ABCdist%','ABCdist%Categoria',"totalVendas","totalVendasCategoria"], axis=1, inplace=True)

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


    def simulacaoProgramacao(self, arraySimulaAbc):

        # Transformando em DataFrame
        dfSimulaAbc = pd.DataFrame({
            'class': arraySimulaAbc[0],
            'percentual': arraySimulaAbc[1]
        })

        abc = self.tendenciaAbc()
        abc = abc.loc[:,
                         ['codItemPai', 'class']]
        tendencia = self.tendenciaVendas('nao')
        tendencia = pd.merge(tendencia,abc,on="codItemPai",how='left')
        tendencia = pd.merge(tendencia, dfSimulaAbc, on='class',how='left')

        tendencia['percentual'].fillna(0, inplace=True)

        tendencia['previcaoVendas'] = tendencia['previcaoVendas'] * (tendencia['percentual']/100)
        tendencia['Prev Sobra'] = (tendencia['emProcesso'] + tendencia['estoqueAtual']) - (
                tendencia['previcaoVendas'] - tendencia['qtdeFaturada'])
        tendencia['faltaProg (Tendencia)'] = tendencia['Prev Sobra'].where(
            tendencia['Prev Sobra'] < 0, 0)

        tendencia['estoqueAtual'] = tendencia['estoqueAtual'].apply(self.formatar_padraoInteiro)
        tendencia['emProcesso'] = tendencia['emProcesso'].apply(self.formatar_padraoInteiro)
        tendencia['qtdeFaturada'] = tendencia['qtdeFaturada'].apply(self.formatar_padraoInteiro)
        tendencia['qtdePedida'] = tendencia['qtdePedida'].apply(self.formatar_padraoInteiro)
        tendencia['previcaoVendas'] = tendencia['previcaoVendas'].apply(self.formatar_padraoInteiro)
        tendencia['disponivel'] = tendencia['disponivel'].apply(self.formatar_padraoInteiro)
        tendencia['faltaProg (Tendencia)'] = tendencia['faltaProg (Tendencia)'].apply(
            self.formatar_padraoInteiro)
        tendencia['Prev Sobra'] = tendencia['Prev Sobra'].apply(self.formatar_padraoInteiro)

        return tendencia

