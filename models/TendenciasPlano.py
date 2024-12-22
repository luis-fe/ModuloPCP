import pandas as pd
from connection import ConexaoPostgreWms

class TendenciaPlano():
    '''Classe utilizada para a analise de tendencias de vendas de um determinado Plano '''

    def __init__(self, codPlano = None, parametroABC = None):

        self.codPlano = codPlano
        self.parametroABC = parametroABC

    def consultaPlanejamentoABC(self):
        '''Metodo utilizado para planejar a distribuicacao ABC'''

    def consultaParametrizacaoABC(self):
        '''Metodo utilizado para consultar a parametrizacao do ABC'''

        sql = """
        select "nomeABC" from pcp."parametroABC"
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


