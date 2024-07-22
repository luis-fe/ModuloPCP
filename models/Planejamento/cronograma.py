import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco




def CronogramaFases(codPlano):

    sql = """
    select plano, codfase as "codFase", datainico as "dataInicio", datafim as "dataFim" from "PCP".pcp.calendario_plano_fases
    where plano = %s
    """
    conn = ConexaoPostgreWms.conexaoEngine()
    cronograma = pd.read_sql(sql,conn,params=(codPlano,))

    # Convertendo as colunas de data para o tipo datetime
    cronograma['dataInicio'] = pd.to_datetime(cronograma['dataInicio'])
    cronograma['dataFim'] = pd.to_datetime(cronograma['dataFim'])

    # Calculando a diferença entre as datas em dias e adicionando como nova coluna
    cronograma['dias'] = (cronograma['dataFim'] - cronograma['dataInicio']).dt.days + 1
    cronograma['codFase'] = cronograma['codFase'].astype(int)
    cronograma['dataFim'] = pd.to_datetime(cronograma['dataFim'], format='%a, %d %b %Y %H:%M:%S %Z')
    cronograma['dataFim'] = cronograma['dataFim'].dt.strftime('%d/%m/%Y')

    cronograma['dataInicio'] = pd.to_datetime(cronograma['dataInicio'], format='%a, %d %b %Y %H:%M:%S %Z')
    cronograma['dataInicio'] = cronograma['dataInicio'].dt.strftime('%d/%m/%Y')

    return cronograma