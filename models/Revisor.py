import pandas as pd

from connection import ConexaoPostgreWms
class Revisor():
    '''Classe para interagir com o cadastro de Revisores na Plataforma '''

    def __init__(self, codRevisor = None, nomeRevisor = None, empresa = None, situacaoRevisor = None):

        self.codRevisor = codRevisor
        self.nomeRevisor = nomeRevisor
        self.empresa = empresa
        self.situacaoRevisor = situacaoRevisor

    def cadastrarRevisor(self):
        '''Metodo utilizado para cadastrar um novo revisor '''

        #Verificar se existe o revisor
        verificar = self.pesquisarRevisorEspecifico()

        if verificar.empty:

            insert = """
            insert into
                pcp."Revisor" ("codRevisor","nomeRevisor", "empresa","situacaoRevisor")
            values
                ( %s, %s , %s, 'Ativo' )
            """

            with ConexaoPostgreWms.conexaoInsercao() as conn:
                with conn.cursor() as curr:
                    curr.execute(insert,(self.codRevisor, self.nomeRevisor,self.empresa))
                    conn.commit()

            return pd.DataFrame([{'status':True,'Mensagem':'Revisor cadastrado com sucesso'}])

        else:
            return pd.DataFrame([{'status':False,'Mensagem':'Revisor ja existe'}])


    def pesquisarRevisorEspecifico(self):
        '''Metodo que pesquisa se um revisor esta cadastrado'''

        select = """
        select 
            * from pcp."Revisor" 
        where "codRevisor" = %s and empresa = %s ;
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select,conn,params=(self.codRevisor,self.empresa))

        return consulta


    def consultaRevisores(self):
        '''Metodo que pesquisa os revisores'''

        select = """
        select 
            * from pcp."Revisor" 
        where  empresa = %s 
        order by "nomeRevisor" ;
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select, conn, params=(self.empresa,))

        return consulta

    def inativarRevisor(self):
        '''Metodo que inativa os revisores'''

        update = """
        update pcp."Revisor"
        set "situacaoRevisor" = %s
        where "codRevisor" = %s
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(update,(self.situacaoRevisor, self.codRevisor,))
                conn.commit()

        return pd.DataFrame([{'status':True,'Mensagem':'StatusRevisor alterado com sucesso'}])


    def exlcuirRevisor(self):
        '''Metodo que exclui o revisor, somente caso nao tenha historico de produtividade'''

        # Verificar se possue historico de produtividade
        consulta = """
        select * from pcp."ProdutividadeRevisor" where empresa = %s and "codRevisor" = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta,conn,params=(self.empresa, self.codRevisor,))

        # IF's
        if consulta.empty:

            #Excluir o revisor
            delete = """
            delete from pcp."Revisor"
            where empresa = % and "codRevisor" = %s
            """

            with ConexaoPostgreWms.conexaoInsercao() as conn:
                with conn.cursor() as curr:
                    curr.execute(delete,(self.empresa, self.codRevisor))
                    conn.commit()

            return pd.DataFrame([{'status':True,'Mensagem':'Revisor excluido com sucesso'}])

        else:

            return pd.DataFrame([{'status':False,'Mensagem':'Revisor ja possue historico'}])







