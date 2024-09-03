import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms
from models import FaccionistaClass
class MetaFaccionista():
    '''Classe utilizada para obter a meta dos faccionistas '''
    def __init__(self, codigoPlano, codLote):

        self.codigoPlano = codigoPlano
        self.codLote = codLote

    def getMetasCategorias(self):
        '''Metodo que obtem as metas por categoria calculado para o Plano de Producao, e obtem o execendte de producao
        return:
        DataFrame: exedente- ({'01- AcordadoDia':[],'categoria':[},)
        '''

        sql = """select * from "backup"."metaCategoria" where "plano" = %s and "codLote" = %s """

        conn = ConexaoPostgreWms.conexaoEngine()
        backupPLano = pd.read_sql(sql, conn, params=(self.codigoPlano, self.codLote))

        faccionista = FaccionistaClass.Faccionista()
        capacidadeFaccionista = faccionista.ConsultarCategoriaMetaFaccionista()
        capacidadeFaccionista.rename(columns={'nomecategoria': 'categoria'}, inplace=True)

        capacidadeFaccionista = pd.merge(backupPLano, capacidadeFaccionista, on='categoria', how='left')
        capacidadeFaccionista['Capacidade/dia'].fillna(0, inplace=True)
        capacidadeFaccionista.fillna('-', inplace=True)
        capacidadeFaccionista['capacidadeSoma'] = capacidadeFaccionista.groupby('categoria')[
            'Capacidade/dia'].transform('sum')

        capacidadeFaccionista['exedente'] = capacidadeFaccionista['Meta Dia'] - capacidadeFaccionista['capacidadeSoma']
        capacidadeFaccionista = capacidadeFaccionista[capacidadeFaccionista['exedente'] > 0].groupby('categoria').agg(
            {'exedente': 'first'}).reset_index()
        capacidadeFaccionista.rename(columns={'exedente': '01- AcordadoDia'}, inplace=True)

        return capacidadeFaccionista


    def RegistroFaccionistas2(self):
        conn = ConexaoPostgreWms.conexaoEngine()

        sql = """SELECT * FROM pcp.faccionista """
        sql2 = """SELECT * FROM pcp."faccaoCategoria" """

        sql = pd.read_sql(sql, conn)
        sql2 = pd.read_sql(sql2, conn)
        merged = pd.merge(sql, sql2, on='codfaccionista', how='left')

        merged.fillna('-', inplace=True)
        merged['nome'] = np.where(merged['apelidofaccionista'] != '-', merged['apelidofaccionista'],
                                  merged['nomefaccionista'])
        merged = merged[['Capacidade/dia', 'codfaccionista', 'nome', 'nomecategoria']]

        # Conversão e renomeação de colunas
        # Substituir valores '-' por NaN
        merged['Capacidade/dia'] = merged['Capacidade/dia'].replace('-', np.nan)

        # Remover valores não numéricos (opcional, se desejar apenas remover linhas inválidas)
        merged = merged[pd.to_numeric(merged['Capacidade/dia'], errors='coerce').notnull()]

        # Substituir valores NaN por 0 (ou outro valor padrão)
        merged['Capacidade/dia'] = merged['Capacidade/dia'].fillna(0)

        # Converter para int
        merged['Capacidade/dia'] = merged['Capacidade/dia'].astype(int)
        merged.rename(columns={'Capacidade/dia': '01- AcordadoDia', 'nomecategoria': 'categoria'}, inplace=True)

        colunas_necessarias = ['01- AcordadoDia', '04-%Capacidade', 'categoria', 'codfaccionista', 'nome',
                               'FaltaProgramar',
                               'Fila', 'dias']
        colunas_existentes = [col for col in colunas_necessarias if col in merged.columns]
        merged = merged.loc[:, colunas_existentes]
        merged['FaltaProgramar'] = merged['FaltaProgramar'] * (merged['04-%Capacidade'] / 100)
        merged['Fila'] = merged['Fila'] * (merged['04-%Capacidade'] / 100)

        return merged



