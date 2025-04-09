import gc
from datetime import datetime
import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms
import pytz
from models import  Cronograma, OrdemProd


class ProducaoFases():
    '''Classe que controla a producao das fases '''

    def __init__(self, periodoInicio = None, periodoFinal = None, codFase = None, dias_buscaCSW = 0, codEmpresa = None, limitPostgres = None, utimosDias = None,
                 arraytipoOPExluir = None, consideraMost ='nao', nomeFase ='', arrayTipoProducao = None):
        '''Contrutor da Classe'''
        self.periodoInicio = periodoInicio
        self.periodoFinal = periodoFinal
        self.codFase = codFase
        self.dias_buscaCSW = dias_buscaCSW
        self.codEmpresa = codEmpresa # codEmpresa - codEmpresa utilizado para consultar os dados
        self.limitPostgres = limitPostgres
        self.utimosDias = utimosDias
        self.arraytipoOPExluir = arraytipoOPExluir # Array com os codigo de tipo de op a serem excluidos da analise
        self.consideraMost = 'nao'
        self.nomeFase = nomeFase
        self.arrayTipoProducao = arrayTipoProducao # Array com os tipo de producao desejado


    def realizadoMediaMovel(self):
        '''Método que retona o realizado por fase de acordo com o periodo informado'''

        realizado = self.__sqlRealizadoPeriodo() # realiza a consulta sql no banco postgre do realizado

        ordemProd = OrdemProd.OrdemProd()

        # 1 - verfica se existe tipo de ops a serem excluidos da analise
        if self.arraytipoOPExluir is not None and isinstance(self.arraytipoOPExluir, list):
            realizado = realizado[~realizado['codtipoop'].isin(self.arraytipoOPExluir)]

        # 2 - verfica o arrayTipoProducao com os tipo de ordens de producao que desejo consultar
        if self.arrayTipoProducao != None:

            agrupamentoOP = ordemProd.agrupado_x_tipoOP()
            dataFrameTipoProducao = pd.DataFrame({'Agrupado': self.arrayTipoProducao})
            dataFrameTipoProducao = pd.merge(agrupamentoOP,dataFrameTipoProducao, on='Agrupado')
            realizado = pd.merge(realizado,dataFrameTipoProducao, on='codtipoop')


        else:
            self.arrayTipoProducao = ['Producao']
            agrupamentoOP = ordemProd.agrupado_x_tipoOP()
            dataFrameTipoProducao = pd.DataFrame({'Agrupado': self.arrayTipoProducao})
            dataFrameTipoProducao = pd.merge(agrupamentoOP,dataFrameTipoProducao, on='Agrupado')
            realizado['codtipoop'] = realizado['codtipoop'].astype(str)
            realizado = pd.merge(realizado,dataFrameTipoProducao, on='codtipoop')


        realizado['filtro'] = realizado['codFase'].astype(str) + '|' + realizado['codEngenharia'].str[0]
        realizado = realizado[(realizado['filtro'] != '401|6')]
        realizado = realizado[(realizado['filtro'] != '401|5')]
        realizado = realizado[(realizado['filtro'] != '426|6')]
        realizado = realizado[(realizado['filtro'] != '441|5')]
        realizado = realizado[(realizado['filtro'] != '412|5')]

        realizado['codFase'] = np.where(realizado['codFase'].isin(['431', '455', '459']), '429', realizado['codFase'])

        realizado = realizado.groupby(["codFase"]).agg({"Realizado": "sum"}).reset_index()

        cronograma = Cronograma.Cronograma()
        diasUteis = cronograma.calcular_dias_uteis(self.periodoInicio, self.periodoFinal)

        # Evitar divisão por zero ou infinito
        realizado['Realizado'] = np.where(diasUteis == 0, 0, realizado['Realizado'] / diasUteis)
        #print(f'dias uteis {diasUteis}')

        return diasUteis

    def __sqlRealizadoPeriodo(self):
        '''Metodo privado que consulta via sql o realizado no banco de dados Postgre '''
        sql = """
        select 
            rf."codEngenharia",
    	    rf.numeroop ,
    	    rf.codfase:: varchar as "codFase", rf."seqRoteiro" , rf."dataBaixa"::date , rf."nomeFaccionista", rf."codFaccionista" , rf."horaMov"::time,
    	    rf."totPecasOPBaixadas" as "Realizado", rf."descOperMov" as operador, rf.chave ,"codtipoop", rf.descricaolote 
        from
    	    pcp.realizado_fase rf 
        where 
    	    rf."dataBaixa"::date >= %s 
    	    and rf."dataBaixa"::date <= %s ;
        """
        conn = ConexaoPostgreWms.conexaoEngineWMSSrv()
        realizado = pd.read_sql(sql, conn, params=(self.periodoInicio, self.periodoFinal,))


        return realizado

    def lotesFiltragrem(self):

        sql = """
            SELECT 
                DISTINCT rf.descricaolote AS filtro
            FROM 
                pcp.realizado_fase rf
            WHERE 
                rf."dataBaixa"::DATE BETWEEN %s AND %s
                AND rf.descricaolote NOT LIKE '%%LOTO%%';
        """
        conn = ConexaoPostgreWms.conexaoEngineWMSSrv()
        realizado = pd.read_sql(sql, conn, params=(self.periodoInicio, self.periodoFinal,))


        realizado['filtro'] = realizado['filtro'].str.replace('LOTE INTERNO ','')
        realizado['filtro'] = realizado['filtro'].str.replace('PRODUÇÃO ','')
        realizado['filtro'].fillna('-',inplace = True)
        realizado.loc[realizado["filtro"].str.contains("ENCOMENDA", na=False), "filtro"] = "ENCOMENDA"
        realizado.loc[realizado["filtro"].str.contains(" TH", na=False), "filtro"] = "ENCOMENDA"
        realizado.loc[realizado["filtro"].str.contains(" QUIOSQUE", na=False), "filtro"] = "ENCOMENDA"

        realizado['filtro'] = realizado['filtro'].str.replace('Á','A')
        realizado['filtro'] = realizado['filtro'].str.replace('ÃO','AO')

        return realizado.drop_duplicates()


    def realizadoFasePeriodo(self):

        realizado = self.__sqlRealizadoPeriodo()
        print(f'nomeFase:{self.nomeFase}')



        if self.arraytipoOPExluir is not None and isinstance(self.arraytipoOPExluir, list):
            realizado = realizado[~realizado['codtipoop'].isin(self.arraytipoOPExluir)]

        if self.consideraMost == 'nao':
            realizado = realizado[~realizado['descricaolote'].str.contains("MOST", case=False, na=False)].reset_index()

        realizado['filtro'] = realizado['codFase'].astype(str) + '|' + realizado['codEngenharia'].str[0]
        realizado = realizado[(realizado['filtro'] != '401|6')]
        realizado = realizado[(realizado['filtro'] != '401|5')]
        realizado = realizado[(realizado['filtro'] != '426|6')]
        realizado = realizado[(realizado['filtro'] != '441|5')]
        realizado = realizado[(realizado['filtro'] != '412|5')]

        # filtrando o nome da fase
        fases = self.__sqlObterFases()

        realizado = pd.merge(realizado, fases , on ="codFase")

        realizado = realizado[realizado["nomeFase"] == str(self.nomeFase)].reset_index()


        realizadoTotal = realizado.groupby(["codFase"]).agg({"Realizado": "sum"}).reset_index()
        realizadoTotal['dataBaixa'] = 'Total:'
        realizadoTotal['dia'] = '-'
        realizado = realizado.groupby(["codFase",'dataBaixa']).agg({"Realizado": "sum"}).reset_index()


        # Convertendo para datetime sem especificar o formato fixo
        realizado["dataBaixa"] = pd.to_datetime(realizado["dataBaixa"], errors="coerce")

        # Criando a coluna formatada no padrão brasileiro
        realizado["dataBaixa"] = realizado["dataBaixa"].dt.strftime("%d/%m/%Y")

        # Criando a coluna com o nome do dia da semana em português
        dias_semana = {
            "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
            "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
        }

        realizado["dia"] = realizado["dataBaixa"].apply(lambda x: dias_semana[pd.to_datetime(x, format="%d/%m/%Y").strftime("%A")])
        realizado = realizado.astype(str)


        realizado = pd.concat([realizado, realizadoTotal], ignore_index=True)

        return realizado




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

    def obterdiaAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return pd.to_datetime(agora)


    def __sqlObterFases(self):

        sql = """
        select
	        distinct "codFase"::varchar ,
	        "nomeFase"
        from
	        "PCP".pcp."Eng_Roteiro" er
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        realizado = pd.read_sql(sql, conn)
        return realizado



    def realizadoFasePeriodoFase(self):

        realizado = self.__sqlRealizadoPeriodo()

        if self.arraytipoOPExluir is not None and isinstance(self.arraytipoOPExluir, list):
            realizado = realizado[~realizado['codtipoop'].isin(self.arraytipoOPExluir)]

        if self.consideraMost == 'nao':
            realizado = realizado[~realizado['descricaolote'].str.contains("MOST", case=False, na=False)].reset_index()

        realizado['filtro'] = realizado['codFase'].astype(str) + '|' + realizado['codEngenharia'].str[0]
        realizado = realizado[(realizado['filtro'] != '401|6')]
        realizado = realizado[(realizado['filtro'] != '401|5')]
        realizado = realizado[(realizado['filtro'] != '426|6')]
        realizado = realizado[(realizado['filtro'] != '441|5')]
        realizado = realizado[(realizado['filtro'] != '412|5')]

        # filtrando o nome da fase
        fases = self.__sqlObterFases()

        realizado = pd.merge(realizado, fases , on ="codFase")

        realizado = realizado[realizado["nomeFase"] == str(self.nomeFase)].reset_index()


        realizadoTotal = realizado.groupby(["nomeFase"]).agg({"Realizado": "sum"}).reset_index()
        realizadoTotal['dataBaixa'] = 'Total:'
        realizadoTotal['dia'] = '-'
        realizado = realizado.groupby(["codFase",'dataBaixa']).agg({"Realizado": "sum"}).reset_index()


        # Convertendo para datetime sem especificar o formato fixo
        realizado["dataBaixa"] = pd.to_datetime(realizado["dataBaixa"], errors="coerce")

        # Criando a coluna formatada no padrão brasileiro
        realizado["dataBaixa"] = realizado["dataBaixa"].dt.strftime("%d/%m/%Y")

        # Criando a coluna com o nome do dia da semana em português
        dias_semana = {
            "Monday": "segunda-feira", "Tuesday": "terça-feira", "Wednesday": "quarta-feira",
            "Thursday": "quinta-feira", "Friday": "sexta-feira", "Saturday": "sábado", "Sunday": "domingo"
        }

        realizado["dia"] = realizado["dataBaixa"].apply(lambda x: dias_semana[pd.to_datetime(x, format="%d/%m/%Y").strftime("%A")])
        realizado = realizado.astype(str)


        realizado = pd.concat([realizado, realizadoTotal], ignore_index=True)

        return realizado



