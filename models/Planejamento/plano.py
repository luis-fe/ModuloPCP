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

def ObeterPlanos():
    conn = ConexaoPostgreWms.conexaoEngine()
    planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)
    planos.rename(
        columns={'codigo': '01- Codigo Plano', 'descricaoPlano': '02- Descricao do Plano', 'inicioVenda': '03- Inicio Venda',
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

    sqlTipoNotasPlano = """select "tipo nota"||'-'||nome as "tipoNota" , plano as "01- Codigo Plano"  from pcp."tipoNotaporPlano" tnp """

    lotes = pd.read_sql(sqlLoteporPlano, conn)
    TipoNotas = pd.read_sql(sqlTipoNotasPlano, conn)


    lotes['01- Codigo Plano'] = lotes['01- Codigo Plano'].astype(str)

    merged = pd.merge(planos, lotes, on='01- Codigo Plano', how='left')
    merged = pd.merge(merged, TipoNotas, on='01- Codigo Plano', how='left')

    # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
    grouped = merged.groupby(['01- Codigo Plano', '02- Descricao do Plano', '03- Inicio Venda', '04- Final Venda',
                              '05- Inicio Faturamento', '06- Final Faturamento', '07- Usuario Gerador', '08- Data Geracao']).agg({
        'lote': lambda x: list(x.dropna().astype(str).unique()),
        'nomelote': lambda x: list(x.dropna().astype(str).unique()),
        'tipoNota': lambda x: list(x.dropna().astype(str).unique())
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
            '10- nomelote': row['nomelote'],
            '11-TipoNotas':row['tipoNota']
        }
        result.append(entry)

    return result

def ObeterPlanosPlano(codigoPlano):
    conn = ConexaoPostgreWms.conexaoEngine()
    planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)
    planos.rename(
        columns={'codigo': '01- Codigo Plano', 'descricaoPlano': '02- Descricao do Plano', 'inicioVenda': '03- Inicio Venda',
                 'FimVenda': '04- Final Venda', "inicoFat": "05- Inicio Faturamento", "finalFat": "06- Final Faturamento",
                 'usuarioGerador': '07- Usuario Gerador', 'dataGeracao': '08- Data Geracao'},
        inplace=True)
    planos.fillna('-', inplace=True)
    planos = planos[planos['01- Codigo Plano']==codigoPlano].reset_index()
    sqlLoteporPlano = """
    select
        plano as "01- Codigo Plano",
        lote,
        nomelote
    from
        "PCP".pcp."LoteporPlano"
    """

    sqlTipoNotasPlano = """select "tipo nota"||'-'||nome as "tipoNota" , plano as "01- Codigo Plano"  from pcp."tipoNotaporPlano" tnp """

    lotes = pd.read_sql(sqlLoteporPlano, conn)
    TipoNotas = pd.read_sql(sqlTipoNotasPlano, conn)


    lotes['01- Codigo Plano'] = lotes['01- Codigo Plano'].astype(str)

    merged = pd.merge(planos, lotes, on='01- Codigo Plano', how='left')
    merged = pd.merge(merged, TipoNotas, on='01- Codigo Plano', how='left')

    # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
    grouped = merged.groupby(['01- Codigo Plano', '02- Descricao do Plano', '03- Inicio Venda', '04- Final Venda',
                              '05- Inicio Faturamento', '06- Final Faturamento', '07- Usuario Gerador', '08- Data Geracao']).agg({
        'lote': lambda x: list(x.dropna().astype(str).unique()),
        'nomelote': lambda x: list(x.dropna().astype(str).unique()),
        'tipoNota': lambda x: list(x.dropna().astype(str).unique())
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
            '10- nomelote': row['nomelote'],
            '11-TipoNotas':row['tipoNota']
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

        insert = """INSERT INTO pcp."Plano" ("codigo","descricaoPlano","inicioVenda","FimVenda","inicoFat", "finalFat", "usuarioGerador","dataGeracao") 
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

        # Deletando caso ja exista vinculo do lote no planto
        deleteVinculo = """Delete from pcp."LoteporPlano" where "lote" = %s AND plano = %s """
        insert = """insert into pcp."LoteporPlano" ("empresa", "plano","lote", "nomelote") values (%s, %s, %s, %s  )"""
        delete = """Delete from pcp.lote_itens where "codLote" = %s """
        conn = ConexaoPostgreWms.conexaoInsercao()
        cur = conn.cursor()

        for lote in arrayCodLoteCsw:
            nomelote = loteCsw.ConsultarLoteEspecificoCsw(empresa,lote)
            cur.execute(deleteVinculo,(lote,codigoPlano,))
            conn.commit()
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
    if mes == 'L':
        mes1 = '/07/'
    elif mes == 'G':
        mes1 = '/08/'
    else:
        mes1 = '//'
    return +lote[3:5]+ mes1 + '20' + lote[:2]  + '-' + nomeLote


def ConsultarLotesVinculados(plano):
    sql = """SELECT plano, lote, nomelote, p."descricaoPlano" 
             FROM pcp."LoteporPlano" l
             INNER JOIN pcp."Plano" p ON p.codigo = l.plano
             WHERE plano = %s"""

    conn = ConexaoPostgreWms.conexaoEngine()
    try:
        df = pd.read_sql(sql, conn, params=(plano,))
        df['nomelote'] = df.apply(lambda r: IncluirData(r['lote'][2] if len(r['lote']) > 2 else '', r['nomelote'],r['lote']),
                                  axis=1)
    finally:
        conn.dispose()

    return df

def ConsultarTipoNotasVinculados(plano):
    sql = """select "tipo nota","tipo nota"||'-'||nome as "Descricao" , plano  from pcp."tipoNotaporPlano" tnp  WHERE plano = %s """
    conn = ConexaoPostgreWms.conexaoEngine()
    sql = pd.read_sql(sql,conn,params=(plano,))

    return sql

def AlterPlano(codigoPlano, descricaoPlano, iniVendas, fimVendas, iniFat, fimFat):
    # Validando se o Plano ja existe
    validador = ConsultaPlano()
    validador = validador[validador['codigo'] == codigoPlano].reset_index()

    if validador.empty:

        return pd.DataFrame([{'Status':False,'Mensagem':'O Plano Informado nao existe'}])
    else:
        descricaoPlanoAtual = validador['descricaoPlano'][0]
        if descricaoPlanoAtual == descricaoPlano or descricaoPlano == '-':
            descricaoPlano = descricaoPlanoAtual

        iniVendasAtual = validador['inicioVenda'][0]
        if iniVendasAtual == iniVendas or iniVendas == '-':
            iniVendas = iniVendasAtual

        FimVendaAtual = validador['FimVenda'][0]
        if FimVendaAtual == fimVendas or fimVendas == '-':
            fimVendas = FimVendaAtual

        inicoFatAtual = validador['inicoFat'][0]
        if inicoFatAtual == iniFat or iniFat == '-':
            iniFat = inicoFatAtual

        finalFatAtual = validador['finalFat'][0]
        if finalFatAtual == fimFat or fimFat == '-':
            fimFat = finalFatAtual


        update = """update "PCP".pcp."Plano"  set "descricaoPlano" = %s , "inicioVenda" = %s , "FimVenda" = %s , "inicoFat" = %s , "finalFat" = %s
        where "codigo" = %s
        """

        conn = ConexaoPostgreWms.conexaoInsercao()
        cur = conn.cursor()
        cur.execute(update, (descricaoPlano, iniVendas, fimVendas, iniFat, fimFat, codigoPlano,))
        conn.commit()
        cur.close()
        conn.close()
        return pd.DataFrame([{'Status':True,'Mensagem':'O Plano foi alterado com sucesso !'}])

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