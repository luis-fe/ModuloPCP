'''
MODULO FEITO PARA CROD DO PLANO - CONJUNTO DE REGRAS QUE FORMAM A POLITICA A SER PROJETADA E SIMULADA
'''

from connection import ConexaoPostgreWms, ConexaoBanco
from datetime import datetime
import pandas as pd
import pytz
from models.Planejamento import loteCsw
def obterdiaAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%d/%m/%Y')
    return agora

def ObeterPlanos():
    conn = ConexaoPostgreWms.conexaoEngine()
    planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)
    planos.rename(
        columns={'codigo': '01- Codigo Plano', 'descricao do Plano': '02- Descricao do Plano', 'inicioVenda': '03- Inicio Venda',
                 'FimVenda': '04- Final Venda', "inicoFat": "05- Inicio Faturamento", "finalFat": "06- Final Faturamento",
                 'usuarioGerador': '07- Usuario Gerador', 'dataGeracao': '08- Data Geracao'},
        inplace=True)
    planos.fillna('-', inplace=True)

    sqlLoteporPlano = """
    select
        plano as "01- Codigo Plano",
        lote,
        nomelote
    from
        "PCP".pcp."LoteporPlano"
    """
    lotes = pd.read_sql(sqlLoteporPlano, conn)

    lotes['01- Codigo Plano'] = lotes['01- Codigo Plano'].astype(str)

    merged = pd.merge(planos, lotes, on='01- Codigo Plano', how='left')

    # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
    grouped = merged.groupby(['01- Codigo Plano', '02- Descricao do Plano', '03- Inicio Venda', '04- Final Venda',
                              '05- Inicio Faturamento', '06- Final Faturamento', '07- Usuario Gerador', '08- Data Geracao']).agg({
        'lote': lambda x: list(x.dropna().astype(str)),
        'nomelote': lambda x: list(x.dropna().astype(str))
    }).reset_index()

    result = []
    for index, row in grouped.iterrows():
        entry = {
            '01- Codigo Plano': row['01- Codigo Plano'],
            '02- Descricao do Plano': row['02- Descricao do Plano'],
            '03- Inicio Venda': row['03- Inicio Venda'],
            '04- Final Venda': row['04- Final Venda'],
            '05- Inicio Faturamento': row['05- Inicio Faturamento'],
            '06- Final Faturamento': row['06- Final Faturamento'],
            '07- Usuario Gerador': row['07- Usuario Gerador'],
            '08- Data Geracao': row['08- Data Geracao'],
            '09- lotes': row['lote'],
            '10- nomelote': row['nomelote']
        }
        result.append(entry)

    return result


def ConsultaPlano():
    conn = ConexaoPostgreWms.conexaoEngine()
    planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)

    return planos


def InserirNovoPlano(codigoPlano, descricaoPlano, iniVendas, fimVendas, iniFat, fimFat, usuarioGerador):

    # Validando se o Plano ja existe
    validador = ConsultaPlano()
    validador = validador[validador['codigo'] == codigoPlano].reset_index()

    if not validador.empty:

        return pd.DataFrame([{'Status':False,'Mensagem':'O Plano ja existe'}])

    else:

        insert = """INSERT INTO pcp."Plano" ("codigo","descricao do Plano","inicioVenda","FimVenda","inicoFat", "finalFat", "usuarioGerador","dataGeracao") 
        values (%s, %s, %s, %s, %s, %s, %s, %s ) """

        data = obterdiaAtual()
        print('data'+data)
        conn = ConexaoPostgreWms.conexaoInsercao()
        cur = conn.cursor()
        cur.execute(insert,(codigoPlano, descricaoPlano, iniVendas, fimVendas, iniFat, fimFat, usuarioGerador, str(data),))
        conn.commit()
        cur.close()
        conn.close()

        return pd.DataFrame([{'Status':True,'Mensagem':'Novo Plano Criado com sucesso !'}])

def VincularLotesAoPlano(codigoPlano, arrayCodLoteCsw):
    empresa = '1'
    # Validando se o Plano ja existe
    validador = ConsultaPlano()
    validador = validador[validador['codigo'] == codigoPlano].reset_index()

    if  validador.empty:

        return pd.DataFrame([{'Status':False,'Mensagem':f'O Plano {codigoPlano} NAO existe'}])
    else:

        insert = """insert into pcp."LoteporPlano" ("empresa", "plano","lote", "nomelote") values (%s, %s, %s, %s  )"""
        delete = """Delete from pcp.lote_itens where "codLote" = %s """
        conn = ConexaoPostgreWms.conexaoInsercao()
        cur = conn.cursor()

        for lote in arrayCodLoteCsw:
            nomelote = loteCsw.ConsultarLoteEspecificoCsw(empresa,lote)
            print(lote)
            cur.execute(insert,(empresa, codigoPlano, lote, nomelote,))
            conn.commit()
            cur.execute(delete,(lote,))
            conn.commit()

        cur.close()
        conn.close()

        loteCsw.ExplodindoAsReferenciasLote(empresa, arrayCodLoteCsw )

        return pd.DataFrame([{'Status': True, 'Mensagem': 'Lotes adicionados ao Plano com sucesso !'}])

def DesvincularLotesAoPlano(codigoPlano, arrayCodLoteCsw):

    empresa = '1'
    # Validando se o Plano ja existe
    validador = ConsultaPlano()
    validador = validador[validador['codigo'] == codigoPlano].reset_index()

    if  validador.empty:

        return pd.DataFrame([{'Status':False,'Mensagem':f'O Plano {codigoPlano} NAO existe'}])
    else:
        for lote in arrayCodLoteCsw:
            loteCsw.DesvincularLotePlano(empresa,lote)

        return pd.DataFrame([{'Status': True, 'Mensagem': 'Lotes Desvinculados do Plano com sucesso !'}])




