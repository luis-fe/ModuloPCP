
### Esse arquivo contem as funcoes de salvar as utimas consulta no banco de dados do POSTGRE , com o
#objetivo especifico de controlar as requisicoes

from connection import ConexaoPostgreWms
from datetime import datetime
import pytz
import pandas as pd

# Funcao Para obter a Data e Hora
def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y %H:%M:%S.%f')[:-3]
    return agora
def TempoUltimaAtualizacaoPCP(dataHoraAtual, rotina):
    conn = ConexaoPostgreWms.conexaoEngine()

    consulta = pd.read_sql('select max(fim) as "ultimaData" from pcp.controle_requisicao_csw crc '
                          "where rotina = %s ", conn, params=(rotina,) )



    utimaAtualizacao = consulta['ultimaData'][0]
    if utimaAtualizacao != None:

        if len(utimaAtualizacao) < 23:
            print(utimaAtualizacao)
            utimaAtualizacao = utimaAtualizacao + '.001'
        else:
            utimaAtualizacao = utimaAtualizacao

    else:
        print('segue o baile')


    if utimaAtualizacao != None:

        # Converte as strings para objetos datetime
        data1_obj = datetime.strptime(dataHoraAtual, "%d/%m/%Y %H:%M:%S.%f")
        data2_obj = datetime.strptime(utimaAtualizacao, "%d/%m/%Y %H:%M:%S.%f")

        # Calcula a diferença entre as datas
        diferenca = data1_obj - data2_obj

        # Obtém a diferença em dias como um número inteiro
        diferenca_em_dias = diferenca.days

        # Obtém a diferença total em segundos
        diferenca_total_segundos = diferenca.total_seconds()

        return diferenca_total_segundos


    else:
        diferenca_total_segundos = 9999
        return diferenca_total_segundos

def salvar(rotina, ip,datahoraInicio):
    datahorafinal = obterHoraAtual()

    # Converte as strings para objetos datetime
    data1_obj = datetime.strptime(datahoraInicio, "%d/%m/%Y %H:%M:%S.%f")
    data2_obj = datetime.strptime(datahorafinal,  "%d/%m/%Y %H:%M:%S.%f")

    # Calcula a diferença entre as datas
    diferenca = data1_obj - data2_obj

    # Obtém a diferença em dias como um número inteiro
    diferenca_em_dias = diferenca.days

    # Obtém a diferença total em segundos
    diferenca_total_segundos = diferenca.total_seconds()
    tempoProcessamento = float(diferenca_total_segundos)


    conn = ConexaoPostgreWms.conexaoInsercao()

    consulta = 'insert into pcp.controle_requisicao_csw (rotina, fim, inicio, ip_origem, "tempo_processamento(s)") ' \
          'values (%s , %s , %s , %s, %s )'

    cursor = conn.cursor()

    cursor.execute(consulta,(rotina,datahorafinal, datahoraInicio, ip, tempoProcessamento ))
    conn.commit()
    cursor.close()




def ExcluirHistorico(diasDesejados):
    conn = ConexaoPostgreWms.conexaoInsercao()

    deletar = "DELETE FROM pcp.controle_requisicao_csw crc " \
              "WHERE rotina = 'Portal Consulta OP' " \
              "AND ((SUBSTRING(fim, 7, 4)||'-'||SUBSTRING(fim, 4, 2)||'-'||SUBSTRING(fim, 1, 2))::date - now()::date) < -%s"

    cursor = conn.cursor()

    cursor.execute(deletar, (diasDesejados,))
    conn.commit()
    cursor.close()
