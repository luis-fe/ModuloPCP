'''
MODULO FEITO PARA CROD DO PLANO - CONJUNTO DE REGRAS QUE FORMAM A POLITICA A SER PROJETADA E SIMULADA
'''

from connection import ConexaoPostgreWms, ConexaoBanco
from datetime import datetime
import pandas as pd
import pytz
from models.Planejamento import loteCsw, TipoNotaCSW
def obterdiaAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y')
    return agora







def ConsultaPlano():
    conn = ConexaoPostgreWms.conexaoEngine()
    planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)

    return planos





def DesvincularLotesAoPlano(codigoPlano, arrayCodLoteCsw):

    empresa = '1'
    # Validando se o Plano ja existe
    validador = ConsultaPlano()
    validador = validador[validador['codigo'] == codigoPlano].reset_index()

    if  validador.empty:

        return pd.DataFrame([{'Status':False,'Mensagem':f'O Plano {codigoPlano} NAO existe'}])
    else:
        for lote in arrayCodLoteCsw:
            loteCsw.DesvincularLotePlano(empresa,lote, codigoPlano)

        return pd.DataFrame([{'Status': True, 'Mensagem': 'Lotes Desvinculados do Plano com sucesso !'}])

def DesvincularNotasAoPlano(codigoPlano, arrayTipoNotas):

    empresa = '1'
    # Validando se o Plano ja existe
    validador = ConsultaPlano()
    validador = validador[validador['codigo'] == codigoPlano].reset_index()

    if  validador.empty:

        return pd.DataFrame([{'Status':False,'Mensagem':f'O Plano {codigoPlano} NAO existe'}])
    else:
        for nota in arrayTipoNotas:
            loteCsw.DesvincularNotaPlano(empresa,nota,codigoPlano)

        return pd.DataFrame([{'Status': True, 'Mensagem': 'Tipo Notas Desvinculados do Plano com sucesso !'}])


def IncluirData(mes, nomeLote,lote):
    if mes == 'R':
        mes1 = '/01/'
    elif mes == 'F':
        mes1 = '/02/'
    elif mes == 'M':
        mes1 = '/03/'
    elif mes == 'A':
        mes1 = '/04/'
    elif mes == 'I':
        mes1 = '/05/'
    elif mes == 'L':
        mes1 = '/06/'
    elif mes == 'L':
        mes1 = '/07/'
    elif mes == 'G':
        mes1 = '/08/'
    elif mes == 'S':
        mes1 = '/09/'
    elif mes == 'O':
        mes1 = '/10/'
    elif mes == 'N':
        mes1 = '/10/'
    elif mes == 'D':
        mes1 = '/12/'
    else:
        mes1 = '//'
    return lote[3:5]+ mes1 + '20' + lote[:2]  + '-' + nomeLote




def ConsultarTipoNotasVinculados(plano):
    sql = """select "tipo nota","tipo nota"||'-'||nome as "Descricao" , plano  from pcp."tipoNotaporPlano" tnp  WHERE plano = %s """
    conn = ConexaoPostgreWms.conexaoEngine()
    sql = pd.read_sql(sql,conn,params=(plano,))

    return sql


def VincularNotasAoPlano(codigoPlano, arrayTipoNotas):
    empresa = '1'
    # Validando se o Plano ja existe
    validador = ConsultaPlano()
    validador = validador[validador['codigo'] == codigoPlano].reset_index()

    if  validador.empty:

        return pd.DataFrame([{'Status':False,'Mensagem':f'O Plano {codigoPlano} NAO existe'}])
    else:
        delete = """DELETE FROM "PCP".pcp."tipoNotaporPlano" WHERE plano = %s and "tipo nota" = %s """

        insert = """INSERT INTO "PCP".pcp."tipoNotaporPlano" ("tipo nota" , plano, nome ) values 
        ( %s, %s, %s )"""

        conn = ConexaoPostgreWms.conexaoInsercao()
        cur = conn.cursor()

        for codNota in arrayTipoNotas:
            nomeNota = TipoNotaCSW.ConsultarTipoNotaEspecificoCsw(int(codNota))
            cur.execute(delete, (codigoPlano, str(codNota),))
            conn.commit()
            cur.execute(insert,(str(codNota), codigoPlano, nomeNota,))
            conn.commit()

        cur.close()
        conn.close()

        return pd.DataFrame([{'Status': True, 'Mensagem': 'TipoNotas adicionados ao Plano com sucesso !'}])