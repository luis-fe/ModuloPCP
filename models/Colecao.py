from connection import ConexaoBanco, ConexaoPostgreWms
import pandas as pd


class Colecao():
    '''Classe utilizada para interagir com a Coleção do sistema de PCP'''
    def __init__(self, codColecao = None, codPlano= None):

        self.codColecao = codColecao
        self.codPlano = codPlano

        if self.codColecao != None:
            self.nomeColecao =  self.obterNomeColecaoCSW()


    def obterColecaoCsw(self):
        '''Metodo utilizado para obter as Colecoes do ERP CSW DA CONSISTEM'''

        get = """
        SELECT
            c.codColecao ,
            c.nome
        FROM
            tcp.Colecoes c
        WHERE
            c.codEmpresa = 1
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(get)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta


    def obterNomeColecaoCSW(self):
        '''Metodo utilizado para obter um Determinado nome da Colecao  do ERP CSW DA CONSISTEM'''

        get = """
        SELECT
            c.codColecao ,
            c.nome
        FROM
            tcp.Colecoes c
        WHERE
            c.codEmpresa = 1 
            and 
            c.codColecao = """+str(self.codColecao)+""""""

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(get)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta['nome'][0]

    def obterColecoesporPlano(self):
        '''Metodo utilizado para obter as Colecoes vinculados a um Plano'''

        get = """
        select
            plano as "codPlano",
            colecao as "codColecao",
            nomecolecao as "nomeColecao"
        from
            "PCP".pcp."colecoesPlano" cp
        where 
            plano = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(get,conn,params=(self.codPlano,))

        return consulta

    def vincularColecaoNoPlano(self):
        '''Metodo utilizado para vincular uma colecao ao plano'''

        # Consultar se a colecao ja foi vinculada:
        verificar = self.obterColecoesporPlano()

        if verificar.empty:

            insert = """
            insert into "PCP".pcp."colecoesPlano" (plano, colecao, nomecolecao) values ( %s, %s, %s )
            """

            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(insert, (self.codPlano, str(self.codColecao),str(self.nomeColecao)))
                    connInsert.commit()

            return pd.DataFrame(
                [{'Status': True, 'Mensagem': f'Colecao {self.codColecao}-{self.nomeColecao} incluida no plano {self.codPlano} !'}])

        else:
            return pd.DataFrame(
                [{'Status': True, 'Mensagem': f'Colecao {self.codColecao}-{self.nomeColecao} incluida no plano {self.codPlano} !'}])


    def vincularArrayColecaoPlano(self, array):

        '''Metodo utilizado para vincular um array de colecao ao plano'''

        for i in array():
            self.codColecao = i
            self.vincularColecaoNoPlano()

        return pd.DataFrame(
            [{'Status': True,
              'Mensagem': f'Colecoes incluida no plano {self.codPlano}  com sucesso!'}])






