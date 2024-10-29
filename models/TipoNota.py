import gc
from connection import ConexaoBanco, ConexaoPostgreWms
import pandas as pd


class Colecao():
    '''Classe utilizada para interagir com a Coleção do sistema de PCP'''
    def __init__(self, codTipoNota = None, codPlano= None):

        self.codTipoNota = codTipoNota
        self.codPlano = codPlano

        if self.codTipoNota != None:
            self.nomeTipoNota =  self.obterNomeColecaoCSW()

    def obtendoTipoNotaCsw(self):
        '''Metodo utilizado para obter os tipo de notas do csw'''

        sql = """
                SELECT 
                    f.codigo , 
                    f.descricao  
                FROM 
                    fat.TipoDeNotaPadrao f
                    """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta

    def obterNomeColecaoCSW(self):
        sql = """SELECT f.codigo , f.descricao  FROM fat.TipoDeNotaPadrao f where codigo = """ + str(self.codTipoNota)

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                nota = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return nota['descricao'][0]

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
        verificar ="""
        select
            plano as "codPlano",
            colecao as "codColecao",
            nomecolecao as "nomeColecao"
        from
            "PCP".pcp."colecoesPlano" cp
        where 
            plano = %s and colecao = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        verificar = pd.read_sql(verificar,conn,params=(self.codPlano,self.codColecao,))

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

        tam = 0
        for i in array:
            self.codColecao = i
            self.nomeColecao = self.obterNomeColecaoCSW()
            self.vincularColecaoNoPlano()
            tam = tam + 1

        return pd.DataFrame(
            [{'Status': True,
              'Mensagem': f'Colecoes incluida no plano {self.codPlano}  com sucesso!'}])






