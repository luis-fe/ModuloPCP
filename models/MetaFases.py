import numpy as np
import pytz
from connection import ConexaoPostgreWms
import pandas as pd
from datetime import datetime
from models import FaturamentoClass, FaseClass, ProducaoFases, Produtos, PlanoClass, Cronograma
from models import OrdemProd
from models.Planejamento import itemsPA_Csw, plano, cronograma
from dotenv import load_dotenv, dotenv_values
import os
import re


class MetaFases():

    '''Classe utilizada para construcao das metas por fase a nivel departamental '''

    def __init__(self, codPlano = None, codLote = None , nomeFase =None, periodoInicio = None , periodoFinal = None, analiseCongelada = False, arrayCodLoteCsw = None,
                 codEmpresa = '1', dataBackupMetas = None):
        '''Construtor da classe'''
        self.codPlano = codPlano # codigo do Plano criado
        self.codLote = codLote   # codigo do Lote criado no PCP
        self.nomeFase = nomeFase # nome da fase
        self.periodoInicio = periodoInicio # filtro do perido de Inicio do realizado
        self.periodoFinal = periodoFinal # filtro do perido final do realizado
        self.analiseCongelada = analiseCongelada # atributo que informa se a analse vai usar recursos pré salvos em csv
        self.arrayCodLoteCsw = arrayCodLoteCsw # Array com o codigo do lote
        self.codEmpresa = codEmpresa
        self.dataBackupMetas = dataBackupMetas

        if arrayCodLoteCsw != None or arrayCodLoteCsw != '':
            self.loteIN = self.transformaando_codLote_clausulaIN() # funcao inicial que defini o loteIN

    def transformaando_codLote_clausulaIN(self):
        '''Metodo que transforma o arrayCodLote em cláusula IN'''

        if self.arrayCodLoteCsw == None:

            return ''
        else:
            nomes_com_aspas = [f"'{nome}'" for nome in self.arrayCodLoteCsw]
            novo = ", ".join(nomes_com_aspas)

            return novo

    def metas_Lote(self):
        '''Metodo que consulta as metas de um lote'''

        # 1.1 - Abrindo a conexao com o Banco
        conn = ConexaoPostgreWms.conexaoEngine()

        # 2.1 sql para pesquisar a previsao a nivel de cor e tam, de acordo com o lote escolhido:

        sqlMetas = """
            SELECT 
                "codLote", 
                "Empresa", 
                "codEngenharia", 
                "codSeqTamanho", 
                "codSortimento", 
                previsao
            FROM 
                "PCP".pcp.lote_itens li
            WHERE 
                "codLote" IN (%s) 
                and "Empresa" = %s
        """

        sqlMetas = pd.read_sql(sqlMetas, conn, params=(self.loteIN, self.codEmpresa))

        return sqlMetas




    def metasFase(self,Codplano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim):
        '''Metodo que consulta as meta por fase'''

        ordemProd = OrdemProd.OrdemProd()

        # 1.0 - Verificando se o usuario está analisando em congelamento , CASE NAO:
        if self.analiseCongelada == False:

            # 2.1 pesquisar a previsao a nivel de cor e tam, de acordo com o lote escolhido:
            sqlMetas = self.metas_Lote()

            # 2.2 - obtem os roteiros das engenharias
            produto = Produtos.Produtos()
            sqlRoteiro = produto.roteiro_Engenharias()

            # 2.3 obtem a ordem de apresentacao de cada fase
            sqlApresentacao = ordemProd.apresentacao_Fases()

            # 2.4 utilizado como apoio para obter a meta a nivel de cor , tamanho, categoria
            consulta = produto.itens_tam_cor()

            # 2.5 Verificar quais codItemPai começam com '1' ou '2'
            mask = consulta['codItemPai'].str.startswith(('1', '2'))
            # 2.5.1 Aplicar as transformações usando a máscara
            consulta['codEngenharia'] = np.where(mask, '0' + consulta['codItemPai'] + '-0', consulta['codItemPai'] + '-0')

            # 2.6 Merge entre os produtos tam e cor e as metas , para descobrir o codigo reduzido (codItem)  dos produtos projetados
            sqlMetas = pd.merge(sqlMetas, consulta, on=["codEngenharia", "codSeqTamanho", "codSortimento"], how='left')
            sqlMetas['codItem'].fillna('-', inplace=True)


            # 3 - Obter o faturamento de um determinado plano e aplicar ao calculo
            consultaPartes = produto.relacaoPartes
            faturado = FaturamentoClass.Faturamento(None,None,None,Codplano, consultaPartes)
            faturadoPeriodo = faturado.faturamentoPeriodo_Plano()


            # 3.1 - Incluindo o faturamento das partes e concatenando com o faturamento dos itens PAI
            faturadoPeriodoPartes = faturado.faturamentoPeriodo_Plano_PartesPeca()
            faturadoPeriodo = pd.concat([faturadoPeriodo, faturadoPeriodoPartes], ignore_index=True)

            # 3.2 - concatenando com o DataFrame das metas o faturmento:
            sqlMetas = pd.merge(sqlMetas,faturadoPeriodo,on='codItem',how='left')


            # 4 - Aplicando os estoques ao calculo
            #----------------------------------------------------------------------------------------------------------
            estoque = produto.estoqueProdutosPA_addPartes()
            sqlMetas = pd.merge(sqlMetas, estoque, on='codItem', how='left')

            # 5- Aplicando a carga em producao
            #-------------------------------------------------------------------------------------------------------
            cargas = ordemProd.carga_porReduzido_addEquivParte(consultaPartes)

            sqlMetas = pd.merge(sqlMetas, cargas, on='codItem', how='left')

            sqlMetas.fillna({
                'saldo': 0,
                'qtdeFaturada': 0,
                'estoqueAtual': 0,
                'carga': 0
            }, inplace=True)
            #--------------------------------------------------------------------------------------------------------

            # 6 Analisando se esta no periodo de faturamento
            diaAtual = datetime.strptime(self.obterDiaAtual(), '%Y-%m-%d')
            plano = PlanoClass.Plano()
            IniFat = plano.iniFat
            IniFat = datetime.strptime(IniFat['inicoFat'][0], '%Y-%m-%d')

            # 6.1 Caso o periodo de faturamento da colecao tenha começado
            if diaAtual >= IniFat:
                sqlMetas['FaltaProgramar1'] = sqlMetas['previsao'] - (
                            sqlMetas['estoqueAtual'] + sqlMetas['carga'] + sqlMetas['qtdeFaturada'])

            # 6.2 caso o faturamento da colecao atual nao tenha iniciado
            else:
                sqlMetas['estoque-saldoAnt'] = sqlMetas['estoqueAtual'] - sqlMetas['saldo']
                sqlMetas['FaltaProgramar1'] = sqlMetas['previsao'] - (sqlMetas['estoque-saldoAnt'] + sqlMetas['carga'])

            # ----------------------------------------------------------------------------------------------------------------

            # 7 - criando a coluna do faltaProgramar , retirando os produtos que tem falta programar negativo
            sqlMetas['FaltaProgramar'] = np.where(sqlMetas['FaltaProgramar1'] > 0, sqlMetas['FaltaProgramar1'], 0)

            # 8 - Salvando os dados para csv que é o retrado da previsao x falta programar a nivel sku
            self.backupsCsv(sqlMetas, 'analise')

            print('excutando a etata 8:Salvando os dados para csv que é o retrado da previsao x falta programar a nivel sku')
            # __________________________________________________________________________________________________________________


            # 9 - Obtendo o falta Programar a por sku, considerando so os PRODUTOS PAI
            Meta = sqlMetas.groupby(["codEngenharia", "codSeqTamanho", "codSortimento", "categoria"]).agg(
                {"previsao": "sum", "FaltaProgramar": "sum"}).reset_index()
            filtro = Meta[Meta['codEngenharia'].str.startswith('0')]
            totalPc = filtro['previsao'].sum()
            totalFaltaProgramar = filtro['FaltaProgramar'].sum()

            #10 Levantando o total da previsao e do que falta programar
            novo2 = self.loteIN.replace('"', "-")
            Totais = pd.DataFrame(
                [{'0-Previcao Pçs': f'{totalPc} pcs', '01-Falta Programar': f'{totalFaltaProgramar} pçs'}])

            #11 Salvando os dados da previsao e do falta programar
            self.backupsCsv(sqlMetas, f'Totais{novo2}')

            # 12 Merge das metas com  o roteiro
            Meta = pd.merge(Meta, sqlRoteiro, on='codEngenharia', how='left')

            # 13 transformamando o codFase e codEngenharia em array para salvar a analise de falta Programar por fase
            codFase_array = Meta['codFase'].values
            codEngenharia_array = Meta['codEngenharia'].values
            # Filtrar as linhas onde 'codFase' é 401
            fase_401 = codFase_array == 401
            fase_426 = codFase_array == 426
            fase_412 = codFase_array == 412
            fase_441 = codFase_array == 441
            # Filtrar as linhas onde 'codEngenharia' não começa com '0'
            nao_comeca_com_0 = np.vectorize(lambda x: not x.startswith('0'))(codEngenharia_array)
            # Combinar as duas condições para filtrar as linhas
            filtro_comb = fase_401 & nao_comeca_com_0
            filtro_comb2 = fase_426 & nao_comeca_com_0
            filtro_comb3 = fase_412 & nao_comeca_com_0
            filtro_comb4 = fase_441 & nao_comeca_com_0
            # Aplicar o filtro invertido
            Meta = Meta[~(filtro_comb | filtro_comb2 | filtro_comb3 | filtro_comb4)]
            self.backupsCsv(Meta, f'analiseFaltaProgrFases')


            # 14 criando o dataFrame das Metas a nivel de fase PREVISAO + FALTAPROGRAMAR
            Meta = Meta.groupby(["codFase", "nomeFase"]).agg({"previsao": "sum", "FaltaProgramar": "sum"}).reset_index()
            Meta = pd.merge(Meta, sqlApresentacao, on='nomeFase', how='left')
            Meta['apresentacao'] = Meta.apply(lambda x: 0 if x['codFase'] == 401 else x['apresentacao'], axis=1)
            Meta = Meta.sort_values(by=['apresentacao'], ascending=True)  # escolher como deseja classificar


            # 15 - Importando o cronograma das fases
            cronogramaS = Cronograma.Cronograma(self.codPlano)
            Meta = pd.merge(Meta, cronogramaS, on='codFase', how='left')


            #16 - Obtendo a Colecao do Lote
            colecoes = self.__tratamentoInformacaoColecao()

            # 17 - Consultando o Fila das fases
            filaFase = ordemProd.filaFases()
            filaFase = filaFase.loc[:,
                       ['codFase', 'Carga Atual', 'Fila']]

            Meta = pd.merge(Meta, filaFase, on='codFase', how='left')

            # 17- formatando erros de validacao nos valores dos atributos
            Meta['Carga Atual'].fillna(0, inplace=True)
            Meta['Fila'].fillna(0, inplace=True)
            Meta['Falta Produzir'] = Meta['Carga Atual'] + Meta['Fila'] + Meta['FaltaProgramar']


            #18 - obtendo a Meta diaria das fases:

            Meta['dias'].fillna(1, inplace=True)
            Meta['Meta Dia'] = Meta['Falta Produzir'] / Meta['dias']
            Meta['Meta Dia'] = Meta['Meta Dia'].round(0)

            # 19 Ponto de Congelamento do lote:
            self.backupsCsv(Meta, f'analiseLote{novo2}')

            # 20 - Buscando o realizado da Producao das fases

            # 21 Carregando o Saldo COLECAO ANTERIOR

    def backupsCsv(self, dataFrame, nome):
        '''Metodo que faz o backup em csv da analise do falta a programar'''

        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        dataFrame.to_csv(f'{caminhoAbsoluto}/dados/{nome}.csv')




    def obterDiaAtual(self):

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return agora


    def previsao_categoria_fase(self):
        '''Metodo que obtem o previsto em cada fase por categoria '''
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')

        previsao = pd.read_csv(f'{caminhoAbsoluto}/dados/analiseFaltaProgrFases.csv')

        previsao = previsao[previsao['nomeFase'] == self.nomeFase].reset_index()
        previsao = previsao.groupby(["categoria"]).agg({"previsao":"sum"}).reset_index()

        previsao = previsao.sort_values(by=['previsao'], ascending=False)  # escolher como deseja classificar

        return previsao


    def faltaProgcategoria_fase(self):
        '''Metodo que obtem o previsto em cada fase por categoria '''
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')

        previsao = pd.read_csv(f'{caminhoAbsoluto}/dados/analiseFaltaProgrFases.csv')

        previsao = previsao[previsao['nomeFase'] == self.nomeFase].reset_index()
        previsao = previsao.groupby(["categoria"]).agg({"FaltaProgramar":"sum"}).reset_index()

        previsao = previsao.sort_values(by=['FaltaProgramar'], ascending=False)  # escolher como deseja classificar

        return previsao


    def cargaProgcategoria_Geral(self):
        '''Metodo que obtem a carga em cada fase por categoria '''


        cargaAtual = """
        select
            o."codFaseAtual",
            o."codreduzido",
            o.total_pcs,
            "codTipoOP", ic.categoria, o."seqAtual" 
        from
            "PCP".pcp.ordemprod o 
        inner join 
            "PCP".pcp.itens_csw ic on ic.codigo = o.codreduzido
        WHERE 
                "codFaseAtual" <> '401'
        """


        conn = ConexaoPostgreWms.conexaoEngine()
        cargaAtual = pd.read_sql(cargaAtual, conn)

        return cargaAtual


    def cargaProgcategoria_fase(self):
        '''Metodo que obtem a carga em cada fase por categoria '''
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')


        cargaAtual = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
        cargaAtual = cargaAtual[cargaAtual['fase']==self.nomeFase].reset_index()


        cargaAtual = cargaAtual[cargaAtual['Situacao']=='em processo'].reset_index()
        cargaAtual = cargaAtual.groupby(["categoria"]).agg({"pcs": "sum"}).reset_index()
        cargaAtual.rename(columns={'pcs': 'Carga'}, inplace=True)


        return cargaAtual
    def __sqlObterFases(self):

        sql = """
        select
	        distinct "codFase"::varchar as "codFaseAtual" ,
	        "nomeFase"
        from
	        "PCP".pcp."Eng_Roteiro" er
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        realizado = pd.read_sql(sql, conn)
        return realizado

    def obterRoteirosFila(self):

        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        roteiro = pd.read_csv(f'{caminhoAbsoluto}/dados/filaroteiroOP.csv')
        roteiro = roteiro[roteiro['fase']==self.nomeFase].reset_index()
        roteiro = roteiro[roteiro['Situacao']=='a produzir'].reset_index()
        roteiro = roteiro.groupby(["categoria"]).agg({"pcs": "sum"}).reset_index()
        roteiro.rename(columns={'pcs': 'Fila'}, inplace=True)

        return roteiro

    def __obterCodFase(self):

        fases = self.__sqlObterFases()
        fases = fases[fases['nomeFase']==self.nomeFase].reset_index()
        retorno = fases['codFaseAtual'][0]
        return retorno


    def faltaProduzirCategoriaFase(self):
        '''Metodo que consulta o falta produzir a nivel de categoria'''

        # 1 - Levantando o falta programar
        faltaProgramar = self.faltaProgcategoria_fase()

        # 2 - Carga
        carga = self.cargaProgcategoria_fase()

        faltaProduzir = pd.merge(faltaProgramar,carga, on ='categoria',how='outer')

        # 3 - Fila
        fila = self.obterRoteirosFila()
        faltaProduzir = pd.merge(faltaProduzir,fila, on ='categoria',how='outer')

        faltaProduzir.fillna(0, inplace = True)
        faltaProduzir['faltaProduzir'] = faltaProduzir['FaltaProgramar'] + faltaProduzir['Carga']+ faltaProduzir['Fila']


        cronogramaS =cronograma.CronogramaFases(self.codPlano)
        codFase = self.__obterCodFase()

        cronogramaS = cronogramaS[cronogramaS['codFase'] == int(codFase)].reset_index()
        print(cronogramaS)

        if not cronogramaS.empty:
            dia_util = cronogramaS['dias'][0]
        else:
            dia_util = 1

        faltaProduzir['metaDiaria'] = faltaProduzir['faltaProduzir'] / dia_util
        faltaProduzir['dias'] = dia_util
        faltaProduzir['metaDiaria'] = faltaProduzir['metaDiaria'].astype(int).round()
        return faltaProduzir



    def calcular_dias_sem_domingos(self,dataInicio, dataFim):
        # Obtendo a data atual
        dataHoje = self.obterdiaAtual()
        # Convertendo as datas para o tipo datetime, se necessário
        if not isinstance(dataInicio, pd.Timestamp):
            dataInicio = pd.to_datetime(dataInicio)
        if not isinstance(dataFim, pd.Timestamp):
            dataFim = pd.to_datetime(dataFim)
        if not isinstance(dataHoje, pd.Timestamp):
            dataHoje = pd.to_datetime(dataFim)

        # Inicializando o contador de dias
        dias = 0
        data_atual = dataInicio

        # Iterando através das datas
        while data_atual <= dataFim:
            # Se o dia não for sábado (5) ou domingo (6), incrementa o contador de dias
            if data_atual.weekday() != 5 and data_atual.weekday() != 6:
                dias += 1
            # Incrementa a data atual em um dia
            data_atual += pd.Timedelta(days=1)

        if dias == 0:
            dias = 1

        return dias

    def __tratamentoInformacaoColecao(self):
        '''Método privado que trata a informacao do nome da colecao'''

        colecoes = []
        lote = OrdemProd.OrdemProd(self.codEmpresa)

        for codLote in self.arrayCodLoteCsw:
            lote.codLote = codLote
            descricaoLote = lote.consultaNomeLote()

            if 'INVERNO' in descricaoLote:
                nome = 'INVERNO' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)
            elif 'PRI' in descricaoLote:
                nome = 'VERAO' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)
            elif 'ALT' in descricaoLote:
                nome = 'ALTO VERAO' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)

            elif 'VER' in descricaoLote:
                nome = 'VERAO' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)
            else:
                nome = 'ENCOMENDAS' + ' ' + self._extrair_ano(descricaoLote)
                colecoes.append(nome)

        return colecoes

    def _extrair_ano(self, descricaoLote):
        '''Metodo privado que extrai da descricao o ano do lote'''

        match = re.search(r'\b2\d{3}\b', descricaoLote)
        if match:
            return match.group(0)
        else:
            return None


    def backupMetasAnteriores(self):
        '''Metodo que busca as metas anteriores '''

        data = str(self.dataBackupMetas).replace('-', '_')
        plano = self.codPlano
        lote = self.transformaando_codLote_clausulaIN()
        lote = """'25F17B'"""

        nome = f'meta_{plano}_{lote}_{data}.csv'
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        dataFrame = pd.read_csv(f'{caminhoAbsoluto}/dados/backup/{nome}')
        dataFrame = dataFrame.loc[:, ['Meta Dia', 'nomeFase']].reset_index()
        dataFrame.rename(
            columns={'Meta Dia':'Meta Anterior'},
            inplace=True)



        return dataFrame


















