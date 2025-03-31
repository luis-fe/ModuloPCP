import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms
from models import OP_CSW, Produto_CSW
import re
from dotenv import load_dotenv, dotenv_values
import os

class OrdemProd():
        '''classe criada para a gestao de OPs da Producao '''

        def __init__(self, codEmpresa = '1', codLote = ''):
            '''Construtor da Classe '''
            self.codEmpresa = codEmpresa # Atributo de empresa
            self.codLote = codLote # atributo com o codLote

            self.opCsw = OP_CSW.OP_CSW(self.codEmpresa, self.codLote)

        def carga_porReduzido(self):
            '''Método que consulta a carga dos itens em aberto'''

            # 1 Consulta sql para obter as OPs em aberto no sistema do ´PCP
            sqlCarga = """
                select 
                    codreduzido as "codItem", 
                    sum(total_pcs) as carga  
                from 
                    pcp.ordemprod o 
                where 
                    codreduzido is not null
                    and 
                    "codProduto" like '0%'
                    and 
                    "codFaseAtual" <> '401'
                group by 
                    codreduzido
            """

            conn = ConexaoPostgreWms.conexaoEngine()
            carga = pd.read_sql(sqlCarga, conn)

            return carga

        def carga_porReduzido_addEquivParte(self, Df_relacaoPartes):
            '''Método que adiciona na consulta de carga o equivalente das partes'''

            # 1 - Consultando a cargaPai

            cargaPai = self.carga_porReduzido()

            # 2 - Obtendo o DE-PARA DAS PARTES:
            partesPecas = Df_relacaoPartes

            # 3 - Realizando o merge
            cargas = cargaPai.copy()  # Criar uma cópia do DataFrame original

            cargaPartes = pd.merge(cargaPai, partesPecas, on='codItem')
            cargaPartes.drop(['codProduto', 'codSeqTamanho', 'codSortimento'], axis=1, inplace=True)

            # Drop do codProduto
            cargaPartes.drop('codItem', axis=1, inplace=True)

            # Rename do redParte para codProduto
            cargaPartes.rename(columns={'redParte': 'codItem'}, inplace=True)

            # concatenando
            cargas = pd.concat([cargas, cargaPartes], ignore_index=True)

            return cargas


        def consultaNomeLote(self):
            '''Metodo que consulta o nome do Lote'''


            nomeLote = self.opCsw.consultarLoteEspecificoCsw()

            return nomeLote

        def ordemProd_geral(self):
            '''Metododo que busca as ordens de producao em aberto do banco pcp'''

            consulta = """
            select 
                o.numeroop as "numeroOP", 
                categoria, 
                sum(o.total_pcs) as pcs 
            from 
                pcp.ordemprod o 
            group by 
                numeroop, categoria
            """

            conn = ConexaoPostgreWms.conexaoEngine()
            consulta = pd.read_sql(consulta, conn)

            return consulta

        def filaFases(self):
            '''Metodo que consulta a fila das fases e retorna um DataFrame com a informacao da fila'''

            # 1: carregando as op em aberto do csw
            sqlOrdemAbertoCsw = self.opCsw.ordemProd_csw_aberto()

            # 2: Carregando os roteiros das OPs em aberto e realizando o merge com a carga (# 01 item anterior)
            fila = self.opCsw.roteiro_ordemProd_csw_aberto()

            #2.1 realizando o merge das informacoes
            fila = pd.merge(fila, sqlOrdemAbertoCsw, on='numeroOP')

            # 3: Encontrando o status das ops em cada fase
            #------------------------------------------------------------------------------------------------------
            fila['codSeqRoteiroAtual'] = fila['codSeqRoteiroAtual'].astype(int) # 3.1 transformando uma coluna em int para fazer comparacoes entre 2 colunas do mesmo tipo
            # 3.2 - Caso o roteiro atual for igual ao codigo da sequencia de roteiro
            fila['Situacao'] = np.where(fila['codSeqRoteiroAtual'] == fila['codSeqRoteiro'], 'em processo', '-') # case TRUE - em processo
            #3.3 - Caso o roteiro atual for maior que a seq roteiro o status é produzido
            fila['Situacao'] = np.where(
                (fila['codSeqRoteiroAtual'] > fila['codSeqRoteiro']) & (fila['Situacao'] == '-'),
                'produzido', fila['Situacao'])
            #3.4 - Caso o roteiro atual for menor que a seq roteiro o status é a produzir
            fila['Situacao'] = np.where(
                (fila['codSeqRoteiroAtual'] < fila['codSeqRoteiro']) & (fila['Situacao'] == '-'),
                'a produzir', fila['Situacao'])
            #--------------------------------------------------------------------------------------------------------

            # 4 - Filtrando para nao utilizar tipo de op nas fases exclusivas
            fila['Situacao'] = np.where((fila['codFase'] == 412) & (fila['tipoOP'] == 4),
                                        'produzido', fila['Situacao'])
            fila['Situacao'] = np.where((fila['codFase'] == 441) & (fila['tipoOP'] == 4),
                                        'produzido', fila['Situacao'])


            # 5 - obtendo os nomes das fases
            sql_nomeFases = self.opCsw.informacoesFasesCsw()

            fila = pd.merge(fila, sql_nomeFases, on='codFase')
            fila['codFaseAtual'] = fila['codFaseAtual'].astype(str)


            # 6 - obtendo o nome da fase atual
            sql_nomeFases2 = self.opCsw.informacoesFasesCsw()
            sql_nomeFases2.rename(columns={'codFase': 'codFaseAtual',"fase": "faseAtual"}, inplace=True)

            sql_nomeFases2['codFaseAtual'] = sql_nomeFases2['codFaseAtual'].astype(str)
            fila = pd.merge(fila, sql_nomeFases2, on='codFaseAtual')

            # 7 - buscando a qtde de peças da OP
            sqlBuscarPecas = self.ordemProd_geral()
            fila = pd.merge(fila, sqlBuscarPecas, on='numeroOP')

            # 8 - acrescentando a colecao e o ano de cada Ordem de Producao
            fila['COLECAO'] = fila['desLote'].apply(self.__tratamentoInformacaoColecao)
            fila['COLECAO'] = fila['COLECAO'] + ' ' + fila['desLote'].apply(self.__extrair_ano)
            fila.fillna('-', inplace=True)

            # 9 - realizando backup em csv dos dados
            self.backupsCsv(fila,'filaroteiroOP')

            #10 - separando as fases que estao na situacao em processo e obtendo a sua respectiva carga
            fila_carga_atual = fila[fila['Situacao'] == 'em processo'].reset_index()
            fila_carga_atual = fila_carga_atual.groupby('codFase').agg({"pcs": 'sum'}).reset_index()
            fila_carga_atual.rename(columns={'pcs': 'Carga Atual'}, inplace=True)

            #11 - obtendo a informacao da situacao em fila : a produzir
            fila_fila = fila[fila['Situacao'] == 'a produzir'].reset_index()
            fila_fila = fila_fila.groupby('codFase').agg({"pcs": 'sum'}).reset_index()
            fila_fila.rename(columns={'pcs': 'Fila'}, inplace=True)

            # 12 - formando um unico DataFrame com a fase + qtdFila + qtdCarga
            fila = fila.groupby('codFase').agg({"fase": 'first'}).reset_index()
            fila = pd.merge(fila, fila_carga_atual, on='codFase', how='left')
            fila = pd.merge(fila, fila_fila, on='codFase', how='left')
            fila.fillna(0, inplace=True)


            # 13 - filtrando apenas as fases que o usuario deseja exibir
            apresentacao = self.apresentacao_Fases()
            apresentacao.rename(columns={'nomeFase': 'fase'}, inplace=True)
            fila = pd.merge(fila, apresentacao, on='fase')

            # 14 - transformando fila e carga atual no tipo Inteiro
            fila['Carga Atual'] = fila['Carga Atual'].astype(int).round()
            fila['Fila'] = fila['Fila'].astype(int).round()


            #15 - Retornando o dataFrame processado
            return fila

        def __tratamentoInformacaoColecao(self, descricaoLote):
            if 'INVERNO' in descricaoLote:
                return 'INVERNO'
            elif 'PRI' in descricaoLote:
                return 'VERAO'
            elif 'ALT' in descricaoLote:
                return 'ALTO VERAO'
            elif 'VER' in descricaoLote:
                return 'VERAO'
            else:
                return 'ENCOMENDAS'

        def __extrair_ano(self, descricaoLote):
            '''Metodo que extrai o ano do lote '''

            match = re.search(r'\b2\d{3}\b', descricaoLote)
            if match:
                return match.group(0)
            else:
                return None

        def backupsCsv(self, dataFrame, nome):
            '''Metodo que faz o backup em csv da analise do falta a programar'''

            load_dotenv('db.env')
            caminhoAbsoluto = os.getenv('CAMINHO')
            dataFrame.to_csv(f'{caminhoAbsoluto}/dados/{nome}.csv')

        def apresentacao_Fases(self):
            '''Metodo que seleciona as fases e a ordem que o usuario quer apresentar no dashboard'''

            # 1.1 - Abrindo a conexao com o Banco
            conn = ConexaoPostgreWms.conexaoEngine()

            # Sql que obtem a ordem de apresentacao de cada fase
            sqlApresentacao = """
            select 
                "nomeFase" , 
                apresentacao  
            from 
                pcp."SeqApresentacao" sa 
            ORDER BY 
                sa.apresentacao
            """

            consulta = pd.read_sql(sqlApresentacao, conn)

            return consulta












