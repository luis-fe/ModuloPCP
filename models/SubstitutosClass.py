from connection import ConexaoPostgreWms
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

