import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco

def calcular_dias_sem_domingos(dataInicio, dataFim):
    # Inicializando o contador de dias
    dias = 0
    data_atual = dataInicio

    # Iterando através das datas
    while data_atual <= dataFim:
        # Se o dia não for domingo, incrementa o contador de dias
        if data_atual.weekday() != 6:
            dias += 1
        # Incrementa a data atual em um dia
        data_atual += pd.Timedelta(days=1)

    return dias


def CronogramaFases(codPlano):
    sql = """
    select plano, codfase as "codFase", datainico as "dataInicio", datafim as "dataFim" from "PCP".pcp.calendario_plano_fases
    where plano = %s
    """
    conn = ConexaoPostgreWms.conexaoEngine()
    cronograma = pd.read_sql(sql, conn, params=(codPlano,))

    # Convertendo as colunas de data para o tipo datetime
    cronograma['dataInicio'] = pd.to_datetime(cronograma['dataInicio'])
    cronograma['dataFim'] = pd.to_datetime(cronograma['dataFim'])

    # Calculando a diferença entre as datas em dias úteis (excluindo domingos) e adicionando como nova coluna
    cronograma['dias'] = cronograma.apply(lambda row: calcular_dias_sem_domingos(row['dataInicio'], row['dataFim']),
                                          axis=1)

    # Convertendo codFase para inteiro
    cronograma['codFase'] = cronograma['codFase'].astype(int)

    # Formatando as colunas de data para o formato desejado
    cronograma['dataFim'] = cronograma['dataFim'].dt.strftime('%d/%m/%Y')
    cronograma['dataInicio'] = cronograma['dataInicio'].dt.strftime('%d/%m/%Y')

    return cronograma