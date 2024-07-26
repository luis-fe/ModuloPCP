import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
import pytz
from datetime import datetime
from models.Planejamento import fases_csw, plano


def obterdiaAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%Y-%m-%d')
    return pd.to_datetime(agora)


def calcular_dias_sem_domingos(dataInicio, dataFim):
    # Obtendo a data atual
    dataHoje = obterdiaAtual()
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

    # Iterando através das datas
    while data_atual <= dataFim:
        # Se o dia não for sábado (5) ou domingo (6), incrementa o contador de dias
        if data_atual.weekday() != 5 and data_atual.weekday() != 6:
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


def ConsultarCronogramaFasesPlano(codigoPlano):

    sql = """select plano , codfase as "codFase" , datainico as "DataInicio" , datafim as "DataFim" from pcp.calendario_plano_fases cpf
    where cpf.plano  = %s order by codfase
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    consulta = pd.read_sql(sql,conn,params=(codigoPlano,))

    fases = fases_csw.Fases()
    consulta = pd.merge(consulta,fases,on='codFase')

    # Convertendo as colunas de data para o tipo datetime
    consulta['DataInicio'] = pd.to_datetime(consulta['DataInicio'])
    consulta['DataFim'] = pd.to_datetime(consulta['DataFim'])

    # Calculando a diferença entre as datas em dias úteis (excluindo domingos) e adicionando como nova coluna
    consulta['dias'] = consulta.apply(lambda row: calcular_dias_sem_domingos(row['DataInicio'], row['DataFim']),
                                          axis=1)

    # Formatando as colunas de data para o formato desejado
    consulta['DataFim'] = consulta['DataFim'].dt.strftime('%d/%m/%Y')
    consulta['DataInicio'] = consulta['DataInicio'].dt.strftime('%d/%m/%Y')


    return consulta


def InserirIntervaloFase(codigoplano, codFase, dataInicio, dataFim):

    # Validando se o Plano ja existe
    validador = plano.ConsultaPlano()
    validador = validador[validador['codigo'] == codigoplano].reset_index()

    if  validador.empty:

        return pd.DataFrame([{'Status':False,'Mensagem':'O Plano NAO existe'}])

    else:
        verificar2 = ConsultarCronogramaFasesPlano(codigoplano)
        verificar2 = verificar2[verificar2['codFase'] == codFase].reset_index()

        if not verificar2.empty:

            update = """
            update pcp.calendario_plano_fases 
            set "datainico" = %s , "datafim" = %s
            where codfase = %s and plano = %s
            """

            conn = ConexaoPostgreWms.conexaoInsercao()
            cur = conn.cursor()
            cur.execute(update, (dataInicio, dataFim,codFase, codigoplano))
            conn.commit()
            cur.close()
            conn.close()
            diasUteis = calcular_dias_sem_domingos(dataInicio, dataFim)

            return pd.DataFrame([{'Status': True, 'Mensagem': 'Inserido com sucesso', 'Dias Uteis': f'{diasUteis}'}])

        else:

            verificar3 = fases_csw.Fases()
            verificar3 = verificar3[verificar3['codFase']==codFase].reset_index()

            if verificar3.empty:
                return pd.DataFrame(
                    [{'Status': False, 'Mensagem': 'Fase Informada Nao existe!'}])
            else:

                insert = """
                insert into pcp.calendario_plano_fases ("plano", "codfase","datainico", "datafim") values ( %s, %s, %s, %s)
                """

                diasUteis = calcular_dias_sem_domingos(dataInicio, dataFim)

                conn = ConexaoPostgreWms.conexaoInsercao()
                cur = conn.cursor()
                cur.execute(insert, (codigoplano, codFase, dataInicio, dataFim,))
                conn.commit()
                cur.close()
                conn.close()

                return pd.DataFrame([{'Status':True, 'Mensagem':'Inserido com sucesso','Dias Uteis':f'{diasUteis}'}])


