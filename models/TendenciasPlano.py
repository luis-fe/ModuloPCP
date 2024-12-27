import pandas as pd
from connection import ConexaoPostgreWms
from models import Vendas
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

        verifica = self.consultaPlanejamentoABC()
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

        # Filtrar categorias diferentes de 'sacola'
        df_filtered = consultaVendasSku[consultaVendasSku['categoria'] != 'Sacola']

        # Somar o acumulado de vendas por marca
        vendas_acumuladas = df_filtered.groupby('marca')['qtdePedida'].sum()

        # Mapear os valores acumulados para o DataFrame original
        consultaVendasSku['vendasAcumuladas'] = consultaVendasSku.apply(
            lambda row: vendas_acumuladas[row['marca']] if row['categoria'] != 'Sacola' else '-', axis=1
        )


        return consultaVendasSku