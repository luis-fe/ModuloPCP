import pandas as pd
from connection import ConexaoPostgreWms

class SimulacaoProg():
    '''Classe utilizada para a simulacao da programacao '''

    def __init__(self, nomeSimulacao = None, classAbc = None, perc_abc = None, categoria = None):

        self.nomeSimulacao = nomeSimulacao
        self.classAbc = classAbc
        self.perc_abc = perc_abc
        self.categoria = categoria

    def inserirSimulacao(self):
        '''metodo que faz a insersao de uma nova simulacao'''

        verifica = self.consultaSimulacao()

        if not verifica.empty:
            return pd.DataFrame([{'status':False, "mensagem": "já existe essa simulacao"}])
        else:
            inserir = '''
                        insert into pcp."Simulacao" ("nomeSimulacao") values ( %s ) 
                        '''
            with ConexaoPostgreWms.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(inserir,(self.nomeSimulacao,))

            return pd.DataFrame([{'status':True, "mensagem": "Simulacao inserida com sucesso"}])



    def consultaSimulacao(self):
        '''metodo que consulta uma simulacao em especifico'''

        select = """
        select "nomeSimulacao" from pcp."Simulacao" where "nomeSimulacao" = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,))

        return consulta


    def inserirAbcSimulacao(self):
        '''metodo que inseri a simulacao nos niveis abc '''

        verfica = self.consultaSimulacaoAbc()

        if not verfica.empty:
            return pd.DataFrame([{'status':False, "mensagem": "já existe essa simulacao"}])

        else:

            insert = """
            insert into pcp."SimulacaoAbc" ("nomeSimulacao", "class", percentual) values ( %s, %s, %s )
            """

            with ConexaoPostgreWms.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(insert,(self.nomeSimulacao,self.classAbc, self.perc_abc))

            return pd.DataFrame([{'status':True, "mensagem": "Simulacao inserida com sucesso"}])



    def consultaSimulacaoAbc(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
            * 
        from 
            "PCP".pcp."SimulacaoAbc" s 
        Where 
            "nomeSimulacao" = %s 
            and class = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,self.classAbc,))

        return consulta


    def consultaSimulacaoCategoria(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
            * 
        from 
            "PCP".pcp."SimulacaoCategoria" s 
        Where 
            "nomeSimulacao" = %s 
            and categoria = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,self.categoria,))

        return consulta

    def consultaSimulacaoMarca(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
            * 
        from 
            "PCP".pcp."SimulacaoMarca" s 
        Where 
            "nomeSimulacao" = %s 
            and marca = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,self.categoria,))

        return consulta

    def get_Simulacoes(self):
        '''metodo que consulta uma simulacao em especifico'''

        select = """
        select "nomeSimulacao" from pcp."Simulacao"
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select, conn )

        return consulta

    def consultaDetalhadaSimulacao(self):
        '''metodo que consulta a simulacao em detalhes'''

        consultaParamentrosAbc = """
        select
            distinct "nomeABC" as class
        from
            "PCP".pcp."Plano_ABC" pa
        order by
            "nomeABC" asc 
        """

        consultaAbcSimulacao = """
        select 
            "class",
            percentual
        from 
            "PCP".pcp."SimulacaoAbc" s 
        Where 
            "nomeSimulacao" = %s 
        """

        consultaCategoriaSimulacao = """
        select 
            "categoria",
            percentual
        from 
            "PCP".pcp."SimulacaoCategoria" s 
        Where 
            "nomeSimulacao" = %s 
        """

        consultaCategoria= """
        select "nomeCategoria" as categoria from "PCP".pcp."Categorias" c 
        where c."nomeCategoria" <> '-'
        order by "nomeCategoria" asc 
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consultaParamentrosAbc = pd.read_sql(consultaParamentrosAbc, conn)
        consultaAbcSimulacao = pd.read_sql(consultaAbcSimulacao,conn , params=(self.nomeSimulacao,))

        consultaSimulacao = pd.merge(consultaParamentrosAbc, consultaAbcSimulacao , on='class', how='left')
        consultaSimulacao['percentual'].fillna(0, inplace=True)

        consultaCategoria = pd.read_sql(consultaCategoria, conn)
        consultaCategoria = pd.mege(consultaCategoria,consultaCategoriaSimulacao,on='categoria', how='left' )
        consultaCategoria['percentual'].fillna(1000, inplace=True)



        consultaSimulacaoNome = self.consultaSimulacao()



        data = {
                '1- Simulacao': f'{consultaSimulacaoNome["nomeSimulacao"][0]}',
                '2- ABC':consultaSimulacao.to_dict(orient='records'),
                '3- Categoria': consultaCategoria.to_dict(orient='records')
        }
        return pd.DataFrame([data])



