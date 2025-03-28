import numpy as np
import pytz
from connection import ConexaoPostgreWms
import pandas as pd
from datetime import datetime
from models import FaturamentoClass, FaseClass, ProducaoFases
from models.GestaoOPAberto import FilaFases
from models.GestaoOPAberto.FilaFases import TratamentoInformacaoColecao
from models.Planejamento import itemsPA_Csw, plano, cronograma
from dotenv import load_dotenv, dotenv_values
import os


class MetaFases():

    '''Classe utilizada para construcao das metas por fase a nivel departamental '''
    def __init__(self, codPlano = None, codLote = None , nomeFase =None, periodoInicio = None , periodoFinal = None ):

        self.codPlano = codPlano
        self.codLote = codLote
        self.nomeFase = nomeFase
        self.periodoInicio = periodoInicio
        self.periodoFinal = periodoFinal
    def metasFase(self,Codplano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado = False):
        '''Metodo que consulta as meta por fase'''

        # 1 - obtendo o codigo do lote a ser considerado
        nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
        novo = ", ".join(nomes_com_aspas)

        # 1.1 - Abrindo a conexao com o Banco
        conn = ConexaoPostgreWms.conexaoEngine()

        # 2.0 - Verificando se o usuario está analisando em congelamento , CASE NAO:
        if congelado == False:

            # 2.1 sql para pesquisar a previsao a nivel de cor e tam, de acordo com o lote escolhido:

            sqlMetas = """
                SELECT 
                    "codLote", "Empresa", "codEngenharia", "codSeqTamanho", "codSortimento", previsao
                FROM 
                    "PCP".pcp.lote_itens li
                WHERE 
                    "codLote" IN (%s)
            """ % novo

            # 2.2 - Sql que obtem os roteiros das engenharias
            sqlRoteiro = """
                select 
                    * 
                from 
                    "PCP".pcp."Eng_Roteiro" er 
            """

            # 2.3 Sql que obtem a ordem de apresentacao de cada fase
            sqlApresentacao = """
            select 
                "nomeFase" , 
                apresentacao  
            from "PCP".pcp."SeqApresentacao" sa 
            """

            # 2.4 Sql utilizado como apoio para obter a meta a nivel de cor , tamanho, categoria
            consulta = """
            select 
                codigo as "codItem", nome, "unidadeMedida" , "codItemPai" , "codSortimento" as "codSortimento" , "codSeqTamanho" as "codSeqTamanho" , categoria 
            from pcp.itens_csw ic 
            """


            sqlMetas = pd.read_sql(sqlMetas,conn)
            sqlRoteiro = pd.read_sql(sqlRoteiro,conn)
            sqlApresentacao = pd.read_sql(sqlApresentacao,conn)

            consulta = pd.read_sql(consulta, conn)

            # Verificar quais codItemPai começam com '1' ou '2'
            mask = consulta['codItemPai'].str.startswith(('1', '2'))
            # Aplicar as transformações usando a máscara
            consulta['codEngenharia'] = np.where(mask, '0' + consulta['codItemPai'] + '-0', consulta['codItemPai'] + '-0')

            sqlMetas = pd.merge(sqlMetas, consulta, on=["codEngenharia", "codSeqTamanho", "codSortimento"], how='left')
            sqlMetas['codItem'].fillna('-', inplace=True)


            # 3 - Obter o faturamento de um determinado plano e aplicar ao calculo
            faturado = FaturamentoClass.Faturamento(None,None,None,Codplano)
            faturadoPeriodo = faturado.faturamentoPeriodo_Plano()
            faturadoPeriodoPartes = faturado.faturamentoPeriodo_Plano_PartesPeca()
            faturadoPeriodo = pd.concat([faturadoPeriodo, faturadoPeriodoPartes], ignore_index=True)

            sqlMetas = pd.merge(sqlMetas,faturadoPeriodo,on='codItem',how='left')

            # 4 - Aplicando os estoques ao calculo
            #----------------------------------------------------------------------------------------------------------
            estoque = itemsPA_Csw.EstoquePartes()
            # 5- Aplicando a carga em producao
            #-------------------------------------------------------------------------------------------------------
            cargaFases = FaseClass.FaseProducao()
            cargas = cargaFases.cargaPartes()

            sqlMetas = pd.merge(sqlMetas, cargas, on='codItem', how='left')

            sqlMetas.fillna({
                'saldo': 0,
                'qtdeFaturada': 0,
                'estoqueAtual': 0,
                'carga': 0
            }, inplace=True)
            #--------------------------------------------------------------------------------------------------------

            # 6 Analisando se esta no periodo de faturamento, e considerando o que falta a entregar de colecoes passadas

            diaAtual = datetime.strptime(self.obterDiaAtual(), '%Y-%m-%d')
            planoAtual = plano.ConsultaPlano()
            planoAtual = planoAtual[planoAtual['codigo'] == Codplano].reset_index()
            IniFat = datetime.strptime(planoAtual['inicoFat'][0], '%Y-%m-%d')


            if diaAtual >= IniFat:
                sqlMetas['FaltaProgramar1'] = sqlMetas['previsao'] - (
                            sqlMetas['estoqueAtual'] + sqlMetas['carga'] + sqlMetas['qtdeFaturada'])

            # 6.2 caso o faturamento da colecao atual nao tenha iniciado
            else:
                sqlMetas['estoque-saldoAnt'] = sqlMetas['estoqueAtual'] - sqlMetas['saldo']
                sqlMetas['FaltaProgramar1'] = sqlMetas['previsao'] - (sqlMetas['estoque-saldoAnt'] + sqlMetas['carga'])

            # 7 - criando a coluna do faltaProgramar , retirando os produtos que tem falta programar negativo
            sqlMetas['FaltaProgramar'] = np.where(sqlMetas['FaltaProgramar1'] > 0, sqlMetas['FaltaProgramar1'], 0)

            # 8 - Salvando os dados para csv que é o retrado da previsao x falta programar a nivel sku
            load_dotenv('db.env')
            caminhoAbsoluto = os.getenv('CAMINHO')
            sqlMetas.to_csv(f'{caminhoAbsoluto}/dados/analise.csv')

            print('excutando a etata 8:Salvando os dados para csv que é o retrado da previsao x falta programar a nivel sku')



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













