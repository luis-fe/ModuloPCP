from connection import ConexaoPostgreWms, ConexaoBanco
import pandas as pd
import fastparquet as fp
from dotenv import load_dotenv, dotenv_values
import os


class Substituto():
    def __init__(self, codMateriaPrima = None , codMateriaPrimaSubstituto = None):

        self.codMateriaPrima = codMateriaPrima
        self.codMateriaPrimaSubstituto = codMateriaPrimaSubstituto

    def consultaSubstitutos(self):
        '''Metodo que consulta todos os substitutos '''

        sql = """
        select 
            codMateriaPrima,
            nomeMatriaPrima,
            codMateriaPrimaSubstituto,
            nomeMatriaPrimaSubstituto
        from
            pcp."SubstituicaoMP"
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def inserirSubstituto(self):
        '''Metodo que insere um substituto'''


    def pesquisarNomeMaterial(self):
        '''Metodo que pesquisa o nome via codigoMaterial'''

        sql = """
    SELECT
        i.nome
    FROM
        cgi.Item2 i2
    inner join cgi.Item i on
        i.codigo = i2.codItem
    WHERE
        i2.Empresa = 1
        and i2.codEditado ='""" + str(self.codMateriaPrima)+"""'"""

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

