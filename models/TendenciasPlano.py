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

    def tendenciaVendasAbc(self):
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
                                                         "codPedido": 'count'}).reset_index()
        consultaVendasSku = consultaVendasSku.sort_values(by=['qtdePedida'],
                                      ascending=False)  # escolher como deseja classificar

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
        consultaVendasSku['vendasAcumuladas'] = consultaVendasSku.apply(
            lambda row: vendas_acumuladas[row['marca']] if row['categoria'] != 'Sacola' else '-', axis=1
        )


        # Mapear os valores acumulados para o DataFrame original
        consultaVendasSku['vendasAcumuladas'] = consultaVendasSku.apply(
            lambda row: row['vendasAcumuladas'] if row['statusAFV'] == 'Normal' else 0, axis=1
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
        return consultaVendasSku