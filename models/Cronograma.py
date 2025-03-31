import pandas as pd
from connection import ConexaoPostgreWms
import pytz
from datetime import datetime

class Cronograma():
    '''Classe que lida com o cronagrama de fases'''

    def __init__(self, codPlano = None, codEmpresa = None):
        '''Construtor da Classe'''

        self.codPlano = codPlano # atributo codPlano
        self.codEmpresa = codEmpresa # atributo codEmpresa

    def get_cronogramaFases(self):

        sql = """
        select 
            plano, 
            codfase as "codFase", 
            datainico as "dataInicio", 
            datafim as "dataFim" 
        from 
            pcp.calendario_plano_fases
        where 
            plano = %s
        """
        conn = ConexaoPostgreWms.conexaoEngine()
        cronograma = pd.read_sql(sql, conn, params=(self.codPlano,))

        # Convertendo as colunas de data para o tipo datetime
        cronograma['dataInicio'] = pd.to_datetime(cronograma['dataInicio'])
        cronograma['dataFim'] = pd.to_datetime(cronograma['dataFim'])

        # Calculando a diferença entre as datas em dias úteis (excluindo domingos) e adicionando como nova coluna
        cronograma['dias'] = cronograma.apply(lambda row: self.calcular_dias_uteis(row['dataInicio'], row['dataFim']),
                                              axis=1)

        # Convertendo codFase para inteiro
        cronograma['codFase'] = cronograma['codFase'].astype(int)

        # Formatando as colunas de data para o formato desejado
        cronograma['dataFim'] = cronograma['dataFim'].dt.strftime('%d/%m/%Y')
        cronograma['dataInicio'] = cronograma['dataInicio'].dt.strftime('%d/%m/%Y')

        return cronograma

    def calcular_dias_uteis(self, dataInicio, dataFim):
        # Obtendo a data atual
        dataHoje = self.obterdiaAtual()
        # Convertendo as datas para o tipo datetime, se necessário
        if not isinstance(dataInicio, pd.Timestamp):
            dataInicio = pd.to_datetime(dataInicio)
        if not isinstance(dataFim, pd.Timestamp):
            dataFim = pd.to_datetime(dataFim)
        if not isinstance(dataHoje, pd.Timestamp):
            dataHoje = pd.to_datetime(dataFim)

        # Ajustando a data de início se for anterior ao dia atual
        if dataHoje > dataInicio:
            dataInicio = dataHoje

        # Inicializando o contador de dias
        dias = 0
        data_atual = dataInicio

        # Obtendo os feriados entre as datas
        feriados = self.tabela_feriados_EntreDatas(dataInicio, dataFim)
        # Convertendo a coluna "data" para datetime, caso necessário
        feriados['data'] = pd.to_datetime(feriados['data'])

        # Iterando através das datas
        while data_atual <= dataFim:
            # Verifica se é dia útil (segunda a sexta) e não é feriado
            if data_atual.weekday() < 5 and data_atual not in feriados['data'].values:
                dias += 1

            # Incrementa a data atual em um dia
            data_atual += pd.Timedelta(days=1)


        return dias

    def obterdiaAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return pd.to_datetime(agora)


    def tabela_feriados_EntreDatas(self, dataInicio, dataFim):
        '''Metodo que organiza os feriados do ano'''

        sql = """
        select
	        "data"::date,
	        "descricaoFeriado"
        from
	        "PCP".pcp."CadastroFeriados" cf 
        where
            "data" >= %s
            and "data" <= %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()

        feriados = pd.read_sql(sql,conn,params=(dataInicio, dataFim))

        return feriados

    def inserirFeriado(self):
        '''Metodo para inserir um feriado'''


    def excluirFeriado(self):
        '''Metodo que exclui o feriado'''










