from connection import ConexaoPostgreWms, ConexaoBanco
import pandas as pd


class Substituto():
    def __init__(self, codMateriaPrima = None , codMateriaPrimaSubstituto = None, nomeCodMateriaPrima = None, nomeCodSubstituto = None):

        self.codMateriaPrima = codMateriaPrima
        self.codMateriaPrimaSubstituto = codMateriaPrimaSubstituto
        self.nomeCodMateriaPrima = nomeCodMateriaPrima
        self.nomeCodSubstituto = nomeCodSubstituto

    def consultaSubstitutos(self):
        '''Metodo que consulta todos os substitutos '''

        sql = """
        select 
            "codMateriaPrima",
            "nomeCodMateriaPrima",
            "codMateriaPrimaSubstituto",
            "nomeCodSubstituto"
        from
            pcp."SubstituicaoMP"
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def inserirSubstituto(self):
        '''Metodo que insere um substituto'''
        self.nomeCodSubstituto = self.pesquisarNomeMaterial(self.codMateriaPrimaSubstituto)
        self.nomeCodMateriaPrima = self.pesquisarNomeMaterial()


        insert = """Insert into pcp."SubstituicaoMP" ("codMateriaPrima" , "nomeCodMateriaPrima" , "codMateriaPrimaSubstituto", "nomeCodSubstituto") 
        values ( %s, %s, %s, %s )"""

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.codMateriaPrima, self.nomeCodMateriaPrima, self.codMateriaPrimaSubstituto, self.nomeCodSubstituto))
                conn.commit()

    def updateSubstituto(self):
        '''Metodo que insere um substituto'''

        self.nomeCodSubstituto = self.pesquisarNomeMaterial(self.codMateriaPrimaSubstituto)

        update = """update  pcp."SubstituicaoMP" 
        set 
            "codMateriaPrimaSubstituto" = %s , "nomeCodSubstituto" =%s
        where 
            "codMateriaPrima" = %s 
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(update,(self.codMateriaPrimaSubstituto, self.nomeCodSubstituto, self.codMateriaPrima))
                conn.commit()



    def pesquisarNomeMaterial(self, codigoMP = ''):
        '''Metodo que pesquisa o nome via codigoMaterial'''


        if codigoMP == '':
            codigoMP = str(self.codMateriaPrima)
        else:
            codigoMP = codigoMP


        sql = """
    SELECT
        i.nome
    FROM
        cgi.Item2 i2
    inner join cgi.Item i on
        i.codigo = i2.codItem
    WHERE
        i2.Empresa = 1
        and i2.codEditado ='""" + codigoMP+"""'"""

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        if consulta.empty:
            return pd.DataFrame([{'status':False, 'nome':'produto nao existe'}])

        else:
            consulta['status'] = True
            return consulta

    def inserirOuAlterSubstitutoMP(self):
        '''Metodo que insere ou altera o substituto de uma materia prima '''

        #1 - Verifica se a materia prima ja possui substituto
        verifica = self.verificarMP()

        if verifica.empty:
            self.inserirSubstituto()
        else:
            self.updateSubstituto()

        return pd.DataFrame([{'status':True,'Mensagem':'Substituto inserido ou alterado com sucesso! '}])

    def verificarMP(self):
        '''Metodo que verifica se a Materia Prima ja possui substituto'''

        sql = """
        select 
            "codMateriaPrima",
            "nomeCodMateriaPrima",
            "codMateriaPrimaSubstituto",
            "nomeCodSubstituto"
        from
            pcp."SubstituicaoMP"
        where
            "codMateriaPrima" = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn, params=(self.codMateriaPrima,))

        return consulta



