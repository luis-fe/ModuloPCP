import gc

import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
from models import FaccionistaClass
from models.GestaoOPAberto import realizadoFases


class MetaFaccionista():
    '''Classe utilizada para obter a meta dos faccionistas '''
    def __init__(self, codigoPlano, codLote,dataMovFaseIni , dataMovFaseFim, congelado):

        self.codigoPlano = codigoPlano
        self.codLote = codLote
        self.dataMovFaseIni = dataMovFaseIni
        self.dataMovFaseFim = dataMovFaseFim
        self.congelado = congelado
    def getExedentePorCategoria(self):
        '''Metodo que obtem as metas por categoria calculado para o Plano de Producao, e obtem o execendte de producao
        return:
        DataFrame: excedente- ({'01- AcordadoDia':[],'categoria':[},)
        '''

        # 1 - Importando dados do backup da metaPorCategoria na fase dos terceirizados
        backupPLano = self.backupMetaCategoriaCalculada()

        # 2 - Na classe faccionita é obtido
        faccionista = FaccionistaClass.Faccionista()
        excedente = faccionista.consultarCategoriaMetaFaccionista_S()

        # 3 - Feito um merge para EXPANDIR o DataFrame por faccionista
        excedente = pd.merge(backupPLano, excedente, on='categoria', how='left')

        # 4 - Tradado a coluna Capacidade/dia para caso vazio retornar 0
        excedente['Capacidade/dia'].fillna(0, inplace=True)

        # - 5 - Trata as outras colunas para '-' caso "NAN" ####
        excedente.fillna('-', inplace=True)

        # 6 - Agupando os dados para obter as soma das capacidades por Categoria
        excedente['capacidadeSoma'] = excedente.groupby('categoria')[
            'Capacidade/dia'].transform('sum')

        # 7 - Obtendo o exedente da categoria : a Quantidade de peças que  esta faltando faccionista
        excedente['exedente'] = excedente['Meta Dia'] - excedente['capacidadeSoma']

        # 8 - identificando o exedente , somente para as categoria onde o exedente é maior que 0
        excedente = excedente[excedente['exedente'] > 0].groupby('categoria').agg(
            {'exedente': 'first'}).reset_index()
        excedente.rename(columns={'exedente': '01- AcordadoDia'}, inplace=True)


        return excedente

    def backupMetaCategoriaCalculada(self):
        '''Metodo que captura o backup do plano calculado a nivel de categoria, para otimizar processamento'''

        sql = """select * from "backup"."metaCategoria" where "plano" = %s and "codLote" = %s """
        conn = ConexaoPostgreWms.conexaoEngine()
        backupPLano = pd.read_sql(sql, conn, params=(self.codigoPlano, self.codLote))
        return backupPLano

    def getMetaFaccionista(self):

        # 1 - Obtendo o que foi excedente
        excedente = self.getExedentePorCategoria()


        #2 - Obtendo os faccionistas cadastrados
        faccionistas = FaccionistaClass.Faccionista()
        faccionistas = faccionistas.consultarCategoriaMetaFaccionista_S()


        #3 - Tratando as informacoes
        faccionistas.rename(columns={'Capacidade/dia': '01- AcordadoDia'}, inplace=True)
        faccionistas['nome'] = np.where(faccionistas['apelidofaccionista'] != '-', faccionistas['apelidofaccionista'],
                                  faccionistas['nomefaccionista'])
        faccionistas['Capacidade/dia'] = faccionistas['Capacidade/dia'].fillna(0)
        faccionistas['Capacidade/dia'] = faccionistas['Capacidade/dia'].astype(int)
        faccionistas.rename(columns={'Capacidade/dia': '01- AcordadoDia', 'nomecategoria': 'categoria','nomefaccionista':'nomefaccionistaCsw'}, inplace=True)


        #4 - Concatenando as informacoes com o exedente no passo #1
        resumo = pd.concat([faccionistas, excedente], ignore_index=True)
        resumo['nome'].fillna('EXCEDENTE', inplace=True)
        resumo.fillna('-', inplace=True)

        # 5 - Encontrando o % de capacidade para cada categoria
        resumo['04-%Capacidade'] = resumo.groupby('categoria')['01- AcordadoDia'].transform('sum')
        resumo['04-%Capacidade'] = round(resumo['01- AcordadoDia'] / resumo['04-%Capacidade'] * 100)
        resumo = resumo.sort_values(by=['categoria', '01- AcordadoDia'], ascending=[True, False])

        # 6 - Carregar backup das metadas por categoria para encontrar a meta  do dia
        consultaBackupMeta = self.backupMetaCategoriaCalculada()
        resumo = pd.merge(resumo, consultaBackupMeta, on='categoria')

        # 7 - Encontrar as colunas necessarias
        colunas_necessarias = ['01- AcordadoDia', '04-%Capacidade', 'categoria', 'codfaccionista', 'nome', 'FaltaProgramar',
                               'Fila','dias','nomefaccionistaCsw']
        colunas_existentes = [col for col in colunas_necessarias if col in resumo.columns]
        resumo = resumo.loc[:, colunas_existentes]
        resumo['FaltaProgramar'] = resumo['FaltaProgramar'] * (resumo['04-%Capacidade'] / 100)
        resumo['Fila'] = resumo['Fila'] * (resumo['04-%Capacidade'] / 100)

        # 8 - carregando a partir do CSW a carga atual das faccoes
        sql = """
            SELECT op.numeroOP, op.codProduto, d.codOP, d.codFac as codfaccionista,l.qtdPecasRem as carga, e.descricao as nome  
        FROM tco.OrdemProd op 
        left join tct.RemessaOPsDistribuicao d on d.Empresa = 1 and d.codOP = op.numeroOP and d.situac = 2 and d.codFase = op.codFaseAtual 
        left join tct.RemessasLoteOP l on l.Empresa = d.Empresa  and l.codRemessa = d.numRem 
        join tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
        WHERE op.codEmpresa =1 and op.situacao =3 and op.codFaseAtual in (455, 459, 429)
            """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                cargaFac = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()
        cargaFac['categoria'] = '-'
        cargaFac['categoria'] = cargaFac['nome'].apply(self.mapear_categoria)
        cargaFac = cargaFac.groupby(['categoria', 'codfaccionista']).agg({'carga': 'sum'}).reset_index()
        cargaFac['codfaccionista'] = cargaFac['codfaccionista'].astype(str)
        resumo = pd.merge(resumo, cargaFac, on=['categoria', 'codfaccionista'], how='left')
        resumo['carga'].fillna(0, inplace=True)


        # 9 Encontrano o Falta Produzir e as metas
        resumo['Falta Produzir'] = resumo[['carga', 'Fila', 'FaltaProgramar']].sum(axis=1)
        resumo['Meta Dia'] = (resumo['Falta Produzir'] / resumo['dias']).round(0)

        # 10 Conferindo se a meta é maior que a capacidade e devolvendo a diferenca para o exedente
        resumo['Meta Dia Exedente'] = resumo.apply(lambda r : r['Meta Dia'] - r['01- AcordadoDia'] if  r['Meta Dia'] > r['01- AcordadoDia']
                                                                                                       and r['nome'] != 'EXCEDENTE' else 0 ,axis=1)
        resumoExcedenteCategoria =  resumo.groupby('categoria')['Meta Dia Exedente'].transform('sum')
        resumo = pd.merge(resumo,resumoExcedenteCategoria,on='categoria', how='left')



        # 11 Resumindo informacoes
        resumo.rename(columns={
            'codfaccionista': '00- codFac',
            'nome': '01-nomeFac',
            'categoria': '03- categoria',
            '01- AcordadoDia': '04- AcordadoDia',
            '04-%Capacidade': '05-%Cap.',
            'FaltaProgramar': '06-FaltaProgramar',
            'Fila': '07-Fila',
            'carga': '08-Carga',
            'Falta Produzir': '09-Falta Produzir',
            'dias': '10-dias',
            'Meta Dia': '11-Meta Dia'}, inplace=True)

        # 12 Obtendo o realizado
        Realizacao = realizadoFases.RemetidoFaseCategoriaFaccionista(self.dataMovFaseIni, self.dataMovFaseFim)
        resumo = pd.merge(resumo, Realizacao, on=['03- categoria', '00- codFac'], how='left')
        resumo['Remetido'].fillna(0, inplace=True)

        Retornado = realizadoFases.RetornadoFaseCategoriaFaccionista(self.dataMovFaseIni, self.dataMovFaseFim)
        resumo = pd.merge(resumo, Retornado, on=['03- categoria', '00- codFac'], how='left')

        return resumo




    def mapear_categoria(self, nome):
            categorias_map = {
                'CAMISA': 'CAMISA',
                'POLO': 'POLO',
                'BATA': 'CAMISA',
                'TRICOT': 'TRICOT',
                'BONE': 'BONE',
                'CARTEIRA': 'CARTEIRA',
                'TSHIRT': 'CAMISETA',
                'REGATA': 'CAMISETA',
                'BLUSAO': 'AGASALHOS',
                'BABY': 'CAMISETA',
                'JAQUETA': 'JAQUETA',
                'CINTO': 'CINTO',
                'PORTA CAR': 'CARTEIRA',
                'CUECA': 'CUECA',
                'MEIA': 'MEIA',
                'SUNGA': 'SUNGA',
                'SHORT': 'SHORT',
                'BERMUDA': 'BERMUDA'
            }
            for chave, valor in categorias_map.items():
                if chave in nome.upper():
                    return valor
            return '-'



