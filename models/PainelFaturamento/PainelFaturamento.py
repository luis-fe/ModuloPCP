'''Arquivo com as regras para o dashboard de painel do faturamento'''

from connection import ConexaoBanco, ConexaoPostgreWms
import datetime
import pytz
import locale
import pandas as pd

def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    dia = agora.strftime('%Y-%m-%d')
    return hora_str, dia
def EncontrandoMesAtual():
    datahora, dia = obterHoraAtual()
    mes = dia[5:7]

    if mes == '01':
        return '01','01'

    else:
        mesAtual = int(mes)
        mesFinal = mesAtual - 1
        mesFinal = str(mesFinal)

        return mesFinal , mes
def obter_notaCsw():
    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute( """select t.codigo ,t.descricao  from Fat.TipoDeNotaPadrao t""")
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            data = pd.DataFrame(rows, columns=colunas)
            del rows

    return data

def ConfTipoNota(empresa):
    conn = ConexaoPostgreWms.conexaoEngine()


    if empresa == 'Todas':
        consulta = pd.read_sql('select distinct tiponota from "pcp"."DashbordTV" c  '
                           "where c.exibi_todas_empresas = 'sim'  ",conn)

    elif empresa == 'Varejo':
        consulta = pd.read_sql('select distinct tiponota from "pcp"."DashbordTV"c  '
                           "where c.empresa = 'Varejo' ",conn)
    elif empresa == 'Outras':
        consulta = pd.read_sql('select distinct tiponota from "pcp"."DashbordTV" c  '
                           "where c.empresa = 'Outras' ",conn)



    else:
        consulta = pd.read_sql('select tiponota from "pcp"."DashbordTV" c '
                           "where c.empresa = %s  ",conn, params=(empresa))

    consulta['tiponota']= consulta['tiponota'].astype(str)


    return consulta


def Faturamento_ano(ano, empresa):
    datahora, dia = obterHoraAtual()
    tipoNota = obter_notaCsw()

    tipoNotaConsiderar = ConfTipoNota(empresa)



    mesFinal, mesAtual = EncontrandoMesAtual()
    if mesAtual == '01':
        dataInicio = ano + '-01-01'
        dataFim = ano + '-12-31'

    else:
        dataInicio = ano + '-'+mesAtual+'-01'
        dataFim = ano + '-12-31'

    if empresa == 'Todas':
        query = 'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado ' \
            'FROM Fat.NotaFiscal n ' \
            'where n.codPedido >= 0 ' \
            'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
            'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 ' \
                                                            ' union ' \
                                                            'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado ' \
            'FROM Fat.NotaFiscal n ' \
            'where n.codTipoDeNota in (30, 180, 156, 51, 175, 81, 12, 47, 67, 149, 159, 1030, 2015, 1, 27, 102, 2, 9998) and codPedido is null ' \
            'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
            'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 ' \


        retornaCsw = """SELECT  i.codPedido, e.vlrSugestao, sum(i.qtdePecasConf) as conf , sum(i.qtdeSugerida) as qtde,  i.codSequencia,
         (SELECT codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = i.codEmpresa and p.codpedido = i.codPedido) as codigo 
         FROM ped.SugestaoPed e 
         inner join ped.SugestaoPedItem i on i.codEmpresa = e.codEmpresa and i.codPedido = e.codPedido and  i.codSequencia= e.codSequencia 
        WHERE
         e.dataGeracao > '2023-01-01' and situacaoSugestao = 2
        group by i.codPedido, e.vlrSugestao,  i.codSequencia """

        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(retornaCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                retornaCsw = pd.DataFrame(rows, columns=colunas)
                del rows
    elif empresa == 'Varejo':
        retornaCsw = """
            SELECT  
                i.codPedido, 
                e.vlrSugestao, 
                SUM(i.qtdePecasConf) AS conf, 
                SUM(i.qtdeSugerida) AS qtde,  
                i.codSequencia,  
                (SELECT codTipoNota  
                 FROM ped.Pedido p 
                 WHERE p.codEmpresa = i.codEmpresa 
                   AND p.codpedido = i.codPedido) AS codigo 
            FROM ped.SugestaoPed e 
            INNER JOIN ped.SugestaoPedItem i 
                ON i.codEmpresa = e.codEmpresa 
               AND i.codPedido = e.codPedido 
            WHERE e.codEmpresa IN (100, 101)
              AND e.dataGeracao > '2023-01-01' 
              AND situacaoSugestao = 2
            GROUP BY 
                i.codPedido, 
                e.vlrSugestao,  
                i.codSequencia
        """

        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(retornaCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                retornaCsw = pd.DataFrame(rows, columns=colunas)
                del rows

        query = 'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado ' \
                'FROM Fat.NotaFiscal n ' \
                'where n.codPedido >= 0 ' \
                'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
                                                                   'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2  and codempresa in (100, 101) ' \
                'union ' \
                        ' select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado ' \
                        ' FROM Fat.NotaFiscal n ' \
                        'where n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
                        ' and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2  and codTipoDeNota in (145)'
    else:
        query = 'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, codPedido, codNumNota, codEmpresa  ' \
            'FROM Fat.NotaFiscal n ' \
            'where n.codPedido >= 0 '   \
            'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
            'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 and codempresa ='+ "'" + empresa + "'" \
                                                            ' union ' \
                                                            "select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, '0' ,codNumNota, "+"'"+empresa+"'" \
            ' FROM Fat.NotaFiscal n ' \
            'where n.codTipoDeNota in (30, 180, 156, 51, 175, 81, 12, 47, 67, 149, 159, 1030, 2015, 1, 27, 102, 2, 9998) and codPedido is null ' \
            'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
            'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 and codempresa ='+ "'" + empresa + "'" \

        retornaCsw = """SELECT  i.codPedido, e.vlrSugestao, sum(i.qtdePecasConf) as conf , sum(i.qtdeSugerida) as qtde,  i.codSequencia,  
         (SELECT codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = i.codEmpresa and p.codpedido = i.codPedido) as codigo 
         FROM ped.SugestaoPed e 
         inner join ped.SugestaoPedItem i on i.codEmpresa = e.codEmpresa and i.codPedido = e.codPedido and i.codsequencia = e.codsequencia 
         WHERE e.codEmpresa = """ + empresa +""" and e.dataGeracao > '2023-01-01' and situacaoSugestao = 2
         group by i.codPedido, e.vlrSugestao,  i.codSequencia """

        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(retornaCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                retornaCsw = pd.DataFrame(rows, columns=colunas)
                del rows

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(query)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            dataframe = pd.DataFrame(rows, columns=colunas)
            del rows

    tipoNota['codigo'] = tipoNota['codigo'].astype(str)
    retornaCsw = pd.merge(retornaCsw, tipoNota, on='codigo')

    retornaCsw["codPedido"] = retornaCsw["codPedido"] + '-' + retornaCsw["codSequencia"]

    # Retirando as bonificacoes
    retornaCswSB = retornaCsw[retornaCsw['codigo'] != 39]
    retornaCswMPLUS = retornaCsw[retornaCsw['codigo'] == 39]

    retornaCswSB = retornaCswSB[retornaCswSB['conf'] == 0]
    retornaCswMPLUS = retornaCswMPLUS[retornaCswMPLUS['conf'] == 0]

    retorna = retornaCswSB['vlrSugestao'].sum()
    retorna = "{:,.2f}".format(retorna)
    retorna = str(retorna)
    retorna = 'R$ ' + retorna.replace(',', ';').replace('.', ',').replace(';', '.')

    ValorRetornaMplus = retornaCswMPLUS['vlrSugestao'].sum()
    ValorRetornaMplus = "{:,.2f}".format(ValorRetornaMplus)
    ValorRetornaMplus = str(ValorRetornaMplus)
    ValorRetornaMplus = 'R$ ' + ValorRetornaMplus.replace(',', ';').replace('.', ',').replace(';', '.')

    nome = './dados/' + ano + 'Vendas' + empresa + '.csv'

    if mesAtual != '01':
        dataframe2 = pd.read_csv(nome)
        dataframe = pd.concat([dataframe,dataframe2],ignore_index=True)
    else:
        dataframe = dataframe
    #dataframe.to_csv('teste.csv')
    dataframe['tiponota'] =dataframe['tiponota'].astype(str)
    dataframe = pd.merge(dataframe, tipoNotaConsiderar, on='tiponota')


    meses = ['01-Janeiro', '02-Fevereiro', '03-Março', '04-Abril', '05-Maio', '06-Junho',
             '07-Julho', '08-Agosto', '09-Setembro', '10-Outubro', '11-Novembro', '12-Dezembro']

    faturamento_por_mes = []
    acumulado = 0.00
    faturamento_acumulado = []

    for mes in meses:
        # Filtrar os dados do mês atual
        procura = f"-{mes.split('-')[0]}-"
        df_mes = dataframe[dataframe['dataEmissao'].str.contains(procura)]

        # Calcular o faturamento do mês
        faturamento_mes = df_mes['faturado'].sum()

        # Acumular o faturamento
        acumulado += faturamento_mes

        # Formatar o faturamento do mês
        faturamento_mes = "{:,.2f}".format(faturamento_mes)
        faturamento_mes = 'R$ ' + faturamento_mes.replace(',', ';').replace('.', ',').replace(';', '.')
        if faturamento_mes == 'R$ 0,00':
            faturamento_mes = ''

        # Formatar o acumulado
        acumulado_str = "{:,.2f}".format(acumulado)
        acumulado_str = 'R$ ' + acumulado_str.replace('.', ';')

        acumulado_str = acumulado_str.replace(',', '.')
        acumulado_str = acumulado_str.replace(';', ',')

        faturamento_por_mes.append(faturamento_mes)
        faturamento_acumulado.append(acumulado_str)

    # Criar um DataFrame com os resultados
    df_faturamento = pd.DataFrame({'Mês': meses, 'Faturado': faturamento_por_mes, 'Fat.Acumulado':faturamento_acumulado})
    total = dataframe['faturado'].sum()
    total = "{:,.2f}".format(total)
    total = 'R$ ' + str(total)
    total = total.replace('.', ";")
    total = total.replace(',', ".")
    total = total.replace(';', ",")
    df_dia = dataframe[dataframe['dataEmissao'].str.contains(dia)]
    df_dia = df_dia['faturado'].sum()
    df_dia = "{:,.2f}".format(df_dia)
    df_dia = 'R$ ' + str(df_dia)
    df_dia = df_dia.replace('.', ";")
    df_dia = df_dia.replace(',', ".")
    df_dia = df_dia.replace(';', ",")

    metaMes, metaTotal = GetMetas(empresa, ano)
    metaTotal = "{:,.2f}".format(metaTotal)
    metaTotal = 'R$ ' + str(metaTotal)
    metaTotal = metaTotal.replace('.', ";")
    metaTotal = metaTotal.replace(',', ".")
    metaTotal = metaTotal.replace(';', ",")

    df_faturamento['meses']= df_faturamento['Mês']

    df_faturamento = pd.merge(df_faturamento, metaMes, on="meses", how='left')
    df_faturamento.drop('meses', axis=1, inplace=True)
    df_faturamento.drop('Mês_x', axis=1, inplace=True)
    df_faturamento['Mês']= df_faturamento['Mês_y']
    df_faturamento.drop('Mês_y', axis=1, inplace=True)


    def format_with_separator(value):
        return locale.format('%0.2f', value, grouping=True)



    df_faturamento['meta'] = df_faturamento['meta'].apply(format_with_separator)


    df_faturamento['meta acum.'] = df_faturamento['meta acum.'].apply(format_with_separator)
    df_faturamento['meta'] = df_faturamento['meta'].astype(str)
    df_faturamento['meta acum.'] = df_faturamento['meta acum.'].astype(str)

    df_faturamento['meta'] = df_faturamento['meta'].str.replace(',', ';')
    df_faturamento['meta acum.'] = df_faturamento['meta acum.'].str.replace(',', ';')

    df_faturamento['meta'] = df_faturamento['meta'].str.replace('.', ',')
    df_faturamento['meta acum.'] = df_faturamento['meta acum.'].str.replace('.', ',')

    df_faturamento['meta'] = 'R$ '+df_faturamento['meta'].str.replace(';', '.')
    df_faturamento['meta acum.'] = 'R$ ' + df_faturamento['meta acum.'].str.replace(';', '.')
    print(df_faturamento['Mês'] )
    df_faturamento['Mês'] = df_faturamento['Mês'].str.split('-', n=1).str[1]

    # Dados que você deseja adicionar
    new_row = {'Mês': '✈TOTAL', 'meta': metaTotal, 'Faturado': total, 'meta acum.': metaTotal, 'Fat.Acumulado': total}

    # Convertendo o dicionário em um DataFrame
    new_row_df = pd.DataFrame([new_row])

    # Concatenando o novo DataFrame com o original
    df_faturamento = pd.concat([df_faturamento, new_row_df], ignore_index=True)

    df_faturamento.fillna('-',inplace=True)

    if empresa == 'Todas':
        data = {
            '1- Ano:': f'{ano}',
            '2- Empresa:': f'{empresa}',
            '3- No Retorna': f"{retorna}",
            '3.1- Retorna Mplus': f"{ValorRetornaMplus}",
            '4- No Dia': f"{df_dia}",
            '5- TOTAL': f"{total}",
            '6- Atualizado as': f"{datahora}",
            '7- Detalhamento por Mes': df_faturamento.to_dict(orient='records')
        }
    else:
        data = {
        '1- Ano:': f'{ano}',
        '2- Empresa:': f'{empresa}',
        '3- No Retorna':f"{retorna}",
        '3.1- Retorna Mplus': f"{ValorRetornaMplus}",
        '4- No Dia': f"{df_dia}",
        '5- TOTAL': f"{total}",
        '6- Atualizado as': f"{datahora}",
        '7- Detalhamento por Mes': df_faturamento.to_dict(orient='records')
    }
    return [data]


def GetMetas(empresa, ano):
    conn = ConexaoPostgreWms.conexaoEngine()
    if empresa != 'Todas':
        consulta = pd.read_sql('select mes as "Mês", meta from "PCP"."DashbordTV".metas '
                           "where empresa = %s and ano = %s   order by mes " , conn, params=(empresa,ano))
    else:
        consulta = pd.read_sql('select mes as "Mês", meta from "PCP"."DashbordTV".metas '
                           "where ano = %s  order by mes" , conn, params=(ano,))
        consulta = consulta.groupby('Mês').agg({
            'Mês': 'first',
            'meta':'sum'

        })

    consulta['meta acum.'] = consulta['meta'].cumsum()

    consulta.fillna('-',inplace=True)
    metaTotal = consulta['meta'].sum()
    consulta['meses'] = consulta['Mês']


    return consulta, metaTotal

def OutrosFat(ano, empresa):
    datahora, dia = obterHoraAtual()
    tipoNota = obter_notaCsw()

    tipoNotaConsiderar = ConfTipoNota(empresa)

    mesFinal, mesAtual = EncontrandoMesAtual()
    dataInicio = ano + '-' + mesAtual + '-01'
    dataFim = ano + '-12-31'
    query = 'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado ' \
            'FROM Fat.NotaFiscal n ' \
            'where n.codTipoDeNota in (48, 167, 30, 118, 102, 149, 168, 170, 159, 156, 12) ' \
            'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
                                                               'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 '

    retornaCsw = """SELECT  i.codPedido, e.vlrSugestao, sum(i.qtdePecasConf) as conf , sum(i.qtdeSugerida) as qtde,  i.codSequencia,  
         (SELECT codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = i.codEmpresa and p.codpedido = i.codPedido) as codigo 
         FROM ped.SugestaoPed e 
         inner join ped.SugestaoPedItem i on i.codEmpresa = e.codEmpresa and i.codPedido = e.codPedido 
         WHERE e.codempresa in (100, 101)
         and e.dataGeracao > '2023-01-01' and situacaoSugestao = 2
         group by i.codPedido, e.vlrSugestao,  i.codSequencia """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(retornaCsw)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            retornaCsw = pd.DataFrame(rows, columns=colunas)
            del rows

            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(query)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            dataframe = pd.DataFrame(rows, columns=colunas)
            del rows

    nome = './dados/' + ano + 'Vendas' + empresa + '.csv'

    dataframe2 = pd.read_csv(nome)

    dataframe = pd.concat([dataframe, dataframe2], ignore_index=True)

    meses = ['01-Janeiro', '02-Fevereiro', '03-Março', '04-Abril', '05-Maio', '06-Junho',
             '07-Julho', '08-Agosto', '09-Setembro', '10-Outubro', '11-Novembro', '12-Dezembro']

    faturamento_por_mes = []
    faturamento_mes_REV = []
    faturamento_mes_DEV = []
    faturamento_mes_IMB = []
    acumulado = 0.00
    acumuladoREV = 0.00
    acumuladoDEV = 0.00
    acumuladoIMB = 0.00
    faturamento_acumulado = []
    faturamento_acumulado_RV = []
    faturamento_acumulado_DEV = []
    faturamento_acumulado_IMB = []

    dataframe48 = dataframe[dataframe['tiponota'] == 48]
    dataframeREV = dataframe[
        (dataframe['tiponota'] == 167) | (dataframe['tiponota'] == 30) | (dataframe['tiponota'] == 118) | (
                    dataframe['tiponota'] == 156)]
    dataframeDEV = dataframe[
        (dataframe['tiponota'] == 102) | (dataframe['tiponota'] == 168) | (dataframe['tiponota'] == 170) |
        (dataframe['tiponota'] == 149) | (dataframe['tiponota'] == 159)]
    dataframeIMOB = dataframe[dataframe['tiponota'] == 12]
    for mes in meses:
        # Filtrar os dados do mês atual
        procura = f"-{mes.split('-')[0]}-"

        df_mes = dataframe48[dataframe48['dataEmissao'].str.contains(procura)]
        df_mesREV = dataframeREV[dataframeREV['dataEmissao'].str.contains(procura)]
        df_mesDEV = dataframeDEV[dataframeDEV['dataEmissao'].str.contains(procura)]
        df_mesIMB = dataframeIMOB[dataframeIMOB['dataEmissao'].str.contains(procura)]

        # Calcular o faturamento do mês
        faturamento_mes = df_mes['faturado'].sum()
        faturamento_mesREV = df_mesREV['faturado'].sum()
        faturamento_mesDev = df_mesDEV['faturado'].sum()
        faturamento_mesIMB = df_mesIMB['faturado'].sum()

        # Acumular o faturamento
        acumulado += faturamento_mes
        # Acumular o faturamento
        acumuladoREV += faturamento_mesREV
        acumuladoDEV += faturamento_mesDev
        acumuladoIMB += faturamento_mesIMB

        # Formatar o faturamento do mês
        faturamento_mes = "{:,.2f}".format(faturamento_mes)
        faturamento_mes = 'R$ ' + faturamento_mes.replace(',', ';').replace('.', ',').replace(';', '.')

        # Formatar o faturamento do mês REV
        faturamento_mesREV = "{:,.2f}".format(faturamento_mesREV)
        faturamento_mesREV = 'R$ ' + faturamento_mesREV.replace(',', ';').replace('.', ',').replace(';', '.')

        # Formatar o faturamento do mês REV
        faturamento_mesDev = "{:,.2f}".format(faturamento_mesDev)
        faturamento_mesDev = 'R$ ' + faturamento_mesDev.replace(',', ';').replace('.', ',').replace(';', '.')

        # Formatar o faturamento do mês IMB
        faturamento_mesIMB = "{:,.2f}".format(faturamento_mesIMB)
        faturamento_mesIMB = 'R$ ' + faturamento_mesIMB.replace(',', ';').replace('.', ',').replace(';', '.')

        if faturamento_mes == 'R$ 0,00':
            faturamento_mes = ''

        if faturamento_mesREV == 'R$ 0,00':
            faturamento_mesREV = ''

        if faturamento_mesDev == 'R$ 0,00':
            faturamento_mesDev = ''

        if faturamento_mesIMB == 'R$ 0,00':
            faturamento_mesIMB = ''

        # Formatar o acumulado
        acumulado_str = "{:,.2f}".format(acumulado)
        acumulado_str = 'R$ ' + acumulado_str.replace('.', ';')

        acumulado_str = acumulado_str.replace(',', '.')
        acumulado_str = acumulado_str.replace(';', ',')

        faturamento_por_mes.append(faturamento_mes)
        faturamento_acumulado.append(acumulado_str)

        # Formatar o acumulado
        acumulado_strRV = "{:,.2f}".format(acumuladoREV)
        acumulado_strRV = 'R$ ' + acumulado_strRV.replace('.', ';')

        acumulado_strRV = acumulado_strRV.replace(',', '.')
        acumulado_strRV = acumulado_strRV.replace(';', ',')

        faturamento_mes_REV.append(faturamento_mesREV)
        faturamento_acumulado_RV.append(acumulado_strRV)

        # Formatar o acumulado
        acumulado_strDV = "{:,.2f}".format(acumuladoDEV)
        acumulado_strDV = 'R$ ' + acumulado_strDV.replace('.', ';')

        acumulado_strDV = acumulado_strDV.replace(',', '.')
        acumulado_strDV = acumulado_strDV.replace(';', ',')

        faturamento_mes_DEV.append(faturamento_mesDev)
        faturamento_acumulado_DEV.append(acumulado_strDV)

        # Formatar o acumulado
        acumulado_strIMB = "{:,.2f}".format(acumuladoIMB)
        acumulado_strIMB = 'R$ ' + acumulado_strIMB.replace('.', ';')

        acumulado_strIMB = acumulado_strIMB.replace(',', '.')
        acumulado_strIMB = acumulado_strIMB.replace(';', ',')

        faturamento_mes_IMB.append(faturamento_mesIMB)
        faturamento_acumulado_IMB.append(acumulado_strIMB)

    # Criar um DataFrame com os resultados
    df_faturamento = pd.DataFrame(
        {'Mês': meses, 'VD Mostruario': faturamento_por_mes, 'VD Mostruario Acumulado': faturamento_acumulado,
         'VD Revenda MP': faturamento_mes_REV, 'VD Rv Acumulado': faturamento_acumulado_RV,
         'DEV MP': faturamento_mes_DEV, 'DEV MP Acumulado': faturamento_acumulado_DEV,
         'VD Imobilizado': faturamento_mes_IMB, 'VD Acum Imobilizado': faturamento_acumulado_IMB})

    df_faturamento['total'] = df_faturamento['VD Mostruario'].str.replace('R\$ ', '').str.replace('.', '').str.replace(
        ',', '.')  # .astype(float)
    df_faturamento['total'] = df_faturamento.apply(lambda row: '0' if row['total'] == '' else row['total'], axis=1)
    df_faturamento['total'] = df_faturamento['total'].astype(float)

    df_faturamento['total1'] = df_faturamento['VD Revenda MP'].str.replace('R\$ ', '').str.replace('.', '').str.replace(
        ',', '.')  # .astype(float)
    df_faturamento['total1'] = df_faturamento.apply(lambda row: '0' if row['total1'] == '' else row['total1'], axis=1)

    df_faturamento['total2'] = df_faturamento['DEV MP'].str.replace('R\$ ', '').str.replace('.', '').str.replace(
        ',', '.')  # .astype(float)
    df_faturamento['total2'] = df_faturamento.apply(lambda row: '0' if row['total2'] == '' else row['total2'], axis=1)

    df_faturamento['total3'] = df_faturamento['VD Imobilizado'].str.replace('R\$ ', '').str.replace('.',
                                                                                                    '').str.replace(
        ',', '.')  # .astype(float)
    df_faturamento['total3'] = df_faturamento.apply(lambda row: '0' if row['total3'] == '' else row['total3'], axis=1)

    df_faturamento['total'] = df_faturamento['total1'].astype(float) + df_faturamento['total'] + df_faturamento[
        'total2'].astype(float) + df_faturamento['total3'].astype(float)
    df_faturamento['totalAcumulado'] = df_faturamento['total'].cumsum() + 1

    def format_with_separator(value):
        return locale.format('%0.2f', value, grouping=True)

    df_faturamento['totalAcumulado'] = df_faturamento['totalAcumulado'].apply(format_with_separator)
    df_faturamento['total'] = df_faturamento['total'].apply(format_with_separator)

    df_faturamento['totalAcumulado'] = 'R$ ' + df_faturamento['totalAcumulado'].str.replace('.', ';').str.replace(',',
                                                                                                                  '.').str.replace(
        ';', ',')
    df_faturamento['total'] = 'R$ ' + df_faturamento['total'].str.replace('.', ';').str.replace(',',
                                                                                                '.').str.replace(
        ';', ',')
    df_faturamento.drop(['total2', 'total1'], axis=1, inplace=True)

    total = dataframe['faturado'].sum()
    total = "{:,.2f}".format(total)
    total = 'R$ ' + str(total)
    total = total.replace('.', ";")
    total = total.replace(',', ".")
    total = total.replace(';', ",")

    totalVDMostruario = dataframe48['faturado'].sum()
    totalVDMostruario = "{:,.2f}".format(totalVDMostruario)
    totalVDMostruario = 'R$ ' + str(totalVDMostruario)
    totalVDMostruario = totalVDMostruario.replace('.', ";")
    totalVDMostruario = totalVDMostruario.replace(',', ".")
    totalVDMostruario = totalVDMostruario.replace(';', ",")

    totalRevenda = dataframeREV['faturado'].sum()
    totalRevenda = "{:,.2f}".format(totalRevenda)
    totalRevenda = 'R$ ' + str(totalRevenda)
    totalRevenda = totalRevenda.replace('.', ";")
    totalRevenda = totalRevenda.replace(',', ".")
    totalRevenda = totalRevenda.replace(';', ",")

    totalDV = dataframeDEV['faturado'].sum()
    totalDV = "{:,.2f}".format(totalDV)
    totalDV = 'R$ ' + str(totalDV)
    totalDV = totalDV.replace('.', ";")
    totalDV = totalDV.replace(',', ".")
    totalDV = totalDV.replace(';', ",")

    totalIMB = dataframeIMOB['faturado'].sum()
    totalIMB = "{:,.2f}".format(totalIMB)
    totalIMB = 'R$ ' + str(totalIMB)
    totalIMB = totalIMB.replace('.', ";")
    totalIMB = totalIMB.replace(',', ".")
    totalIMB = totalIMB.replace(';', ",")

    df_dia = dataframe[dataframe['dataEmissao'].str.contains(dia)]
    df_dia = df_dia['faturado'].sum()
    df_dia = "{:,.2f}".format(df_dia)
    df_dia = 'R$ ' + str(df_dia)
    df_dia = df_dia.replace('.', ";")
    df_dia = df_dia.replace(',', ".")
    df_dia = df_dia.replace(';', ",")
    df_faturamento['Mês'] = df_faturamento['Mês'].str.split('-', 1).str[1]
    df_faturamento = df_faturamento.append(
        {'Mês': '✈TOTAL', 'total': total, 'totalAcumulado': total, 'VD Mostruario': totalVDMostruario,
         'VD Mostruario Acumulado': totalVDMostruario, 'VD Revenda MP': totalRevenda,
         'VD Rv Acumulado': totalRevenda, 'DEV MP': totalDV, 'DEV MP Acumulado': totalDV, 'VD Imobilizado': totalIMB,
         'VD Acum Imobilizado': totalIMB},
        ignore_index=True)

    df_faturamento.fillna('-', inplace=True)

    data = {
        '1- Ano:': f'{ano}',
        '2- Empresa:': f'{empresa}',
        '3- No Retorna': f"{0}",
        '3.1- Retorna Mplus': f"{0}",
        '4- No Dia': f"{df_dia}",
        '5- TOTAL': f"{total}",
        '6- Atualizado as': f"{datahora}",
        '7- Detalhamento por Mes': df_faturamento.to_dict(orient='records')
    }
    return [data]

def Backup(ano, empresa):
    datahora, dia = obterHoraAtual()

    mesFinal, mesAtual = EncontrandoMesAtual()

    dataInicio = ano + '-01-01'
    backupAno = True
    if mesFinal in ['11','4','6','09']:
        dataFim = ano + '-'+mesFinal+'-30'
    elif mesFinal in ['01']:
        backupAno = False
        dataFim = ano + '-'+mesFinal+'-31'
    elif mesFinal in ['02'] or  mesFinal in ['2']:
        dataFim = ano + '-' + mesFinal + '-29'
    else:
        dataFim = ano + '-'+mesFinal+'-31'

    if empresa == 'Todas':

        print(dataInicio)
        print(dataFim)

        query = 'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, codPedido ' \
            'FROM Fat.NotaFiscal n ' \
            'where n.codPedido >= 0 ' \
            'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
            'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 ' \
                                                            ' union ' \
                                                            'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, codPedido ' \
            'FROM Fat.NotaFiscal n ' \
            'where n.codTipoDeNota not in (9) and codPedido is null ' \
            'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
            'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 ' \


        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(query)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                dataframe = pd.DataFrame(rows, columns=colunas)
                del rows

        nome = './dados/'+ ano + 'Vendas'+empresa+'.csv'
        dataframe.to_csv(nome)

    elif empresa =='Varejo':
        query = 'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, codPedido ' \
                'FROM Fat.NotaFiscal n ' \
                'where n.codPedido >= 0 ' \
                'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
                                                                   'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 and codempresa in(100, 101) ' \
                                                                                        'union ' \
                                                                                        ' select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, codPedido ' \
                                                                                        ' FROM Fat.NotaFiscal n ' \
                                                                                        'where n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
                                                                                        ' and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2  and codTipoDeNota in (145)'
        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(query)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                dataframe = pd.DataFrame(rows, columns=colunas)
                del rows

        nome = './dados/'+ ano + 'Vendas'+empresa+'.csv'
        dataframe.to_csv(nome)
    elif empresa == 'Outras':
        query = 'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, codPedido ' \
                'FROM Fat.NotaFiscal n ' \
                'where n.codTipoDeNota in (48, 167, 30, 118, 102, 149, 168, 170, 159, 156, 12) ' \
                'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
                                                                   'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 '
        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(query)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                dataframe = pd.DataFrame(rows, columns=colunas)
                del rows


        nome = './dados/'+ ano + 'Vendas'+empresa+'.csv'
        dataframe.to_csv(nome)

    else:
        if backupAno == True:
            query = 'select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, codPedido, codNumNota, codEmpresa , codpedido ' \
                'FROM Fat.NotaFiscal n ' \
                'where n.codPedido >= 0 '   \
                'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
                'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 and codempresa ='+ "'" + empresa + "'" \
                                                                ' union ' \
                                                                "select n.codTipoDeNota as tiponota, n.dataEmissao, n.vlrTotal as faturado, '0' ,codNumNota, "+"'"+empresa+"', codpedido" \
                ' FROM Fat.NotaFiscal n ' \
                'where n.codTipoDeNota not in (9) and codPedido is null ' \
                'and n.dataEmissao >= ' + "'" + dataInicio + "'" + ' ' \
                'and n.dataEmissao <= ' + "'" + dataFim + "'" + ' and situacao = 2 and codempresa ='+ "'" + empresa + "'" \

            with ConexaoBanco.ConexaoInternoMPL() as conn:
                with conn.cursor() as cursor_csw:
                    # Executa a primeira consulta e armazena os resultados
                    cursor_csw.execute(query)
                    colunas = [desc[0] for desc in cursor_csw.description]
                    rows = cursor_csw.fetchall()
                    dataframe = pd.DataFrame(rows, columns=colunas)
                    del rows
        else:
            dataframe = pd.DataFrame([{'Mensagem':'Sem meses para guardar backup'}])

        nome = './dados/'+ ano + 'Vendas'+empresa+'.csv'
        dataframe.to_csv(nome)
