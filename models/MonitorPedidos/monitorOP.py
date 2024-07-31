'''
Nessa classe é acrescentado os dados referente a ops sku em aberto
'''

import fastparquet as fp
import numpy
from connection import ConexaoPostgreWms, ConexaoBanco
import pandas as pd

# Funcao para organizar o Monitor em datas de embarque (entrega) e atribuir a OP pré reservado para cada sku a nivel de pedido
def ReservaOPMonitor(dataInico, dataFim):
    # Passo 1 : Carregar os dados da OP
    consultaSql = """
    select o.codreduzido as "codProduto", id, "qtdAcumulada", "ocorrencia_sku" from "pcp".ordemprod o where "qtdAcumulada" > 0
    """
    descricaoArquivo = dataInico+'_'+dataFim
    # Carregar o arquivo Parquet com os parametros do monitor de pedidos, caso o usuario opte por filtrar o monitor, acessa o arquivo monitor_filtro
    try:
        parquet_file = fp.ParquetFile(f'./dados/monitor{descricaoArquivo}.parquet')
        # Converter para DataFrame do Pandas
        monitor = parquet_file.to_pandas()
        # disponibiliza um novo arquivo para ser utilizado com filtragem
        fp.write(f'./dados/monitor_filtro.parquet', monitor)
    except:
        parquet_file = fp.ParquetFile(f'./dados/monitor_filtro.parquet')
        # Converter para DataFrame do Pandas
        monitor = parquet_file.to_pandas()

    # Condição para o cálculo da coluna 'NecessodadeOP'
    condicao = (monitor['Qtd Atende'] > 0)
    # Cálculo da coluna 'NecessodadeOP' de forma vetorizada
    monitor['NecessodadeOP'] = numpy.where(condicao, 0, monitor['QtdSaldo'])
    monitor['NecessodadeOPAcum'] = monitor.groupby('codProduto')['NecessodadeOP'].cumsum()

    conn = ConexaoPostgreWms.conexaoEngine()
    consulta = pd.read_sql(consultaSql, conn)

    monitor['id_op'] = '-'
    monitor['Op Reservada'] = '-'
    for i in range(7):
        i = i + 1  # Utilizado para obter a iteracao
        consultai = consulta[consulta['ocorrencia_sku'] == i]  # filtra o dataframe consulta de acordo com a iteracao

        # Apresenta o numero de linhas do dataframe filtrado
        x = consultai['codProduto'].count()
        print(f'iteracao {i}: {x}')

        # Realiza um merge com outro dataFrame Chamado Monitor
        monitor = pd.merge(monitor, consultai, on='codProduto', how='left')
        # Condição para o cálculo da coluna 'id_op'
        condicao = (monitor['NecessodadeOP'] == 0) | (monitor['NecessodadeOPAcum'] <= monitor['qtdAcumulada']) | (
                    monitor['id_op'] == 'Atendeu')
        # Cálculo da coluna 'id_op' de forma vetorizada
        monitor['id_op'] = numpy.where(condicao, 'Atendeu', 'nao atendeu')
        monitor['Op Reservada'] = monitor.apply(
            lambda r: r['id'] if (r['id_op'] == 'Atendeu') & (r['NecessodadeOP'] > 0) & (r['Op Reservada'] == '-') else
            r['Op Reservada'], axis=1)

        # Define NecessodadeOP para 0 onde id_op é 'Atendeu'
        monitor.loc[monitor['id_op'] == 'Atendeu', 'NecessodadeOP'] = 0

        # Remove as colunas para depois fazer novo merge
        monitor = monitor.drop(['id', 'qtdAcumulada','ocorrencia_sku'], axis=1)
    monitor.loc[monitor['Valor Atende por Cor(Distrib.)'] > 0, 'id_op'] = 'AtendeuDistribuido'
    monitor.loc[monitor['QtdSaldo'] == 0, 'id_op'] = 'JaFaturada'

    # Condição para o cálculo da coluna 'Op Reservada'
    condicao = (monitor['Op Reservada'] == '-') & (monitor['id_op'] == 'Atendeu')
    # Atribui '9999||' onde a condição é verdadeira
    monitor.loc[condicao, 'Op Reservada'] = '0994||'
    # Condição para o cálculo da coluna 'Op Reservada'
    condicao = (monitor['Op Reservada'] == '-') & (monitor['id_op'] == 'AtendeuDistribuido')
    # Atribui '9999||' onde a condição é verdadeira
    monitor.loc[condicao, 'Op Reservada'] = '9997||'
    # Condição para o cálculo da coluna 'Op Reservada'
    condicao = (monitor['Op Reservada'] == '-') & (monitor['id_op'] == 'JaFaturada')
    # Atribui '9999||' onde a condição é verdadeira
    monitor.loc[condicao, 'Op Reservada'] = '9999||'

    def avaliar_grupo(df_grupo):
            return len(set(df_grupo)) == 1

    df_resultado = monitor.loc[:, ['Pedido||Prod.||Cor', 'id_op']]
    df_resultado = df_resultado.groupby('Pedido||Prod.||Cor')['id_op'].apply(avaliar_grupo).reset_index()
    df_resultado.columns = ['Pedido||Prod.||Cor', 'ResultadoAva']
    df_resultado['ResultadoAva'] = df_resultado['ResultadoAva'].astype(str)
    monitor = pd.merge(monitor, df_resultado, on='Pedido||Prod.||Cor', how='left')

    # Condição para o cálculo da coluna 'Op Reservada'
    condicao = (monitor['id_op'] == 'JaFaturada') & (monitor['ResultadoAva'] == 'False')
    monitor.loc[condicao, 'Op Reservada'] = '9998||'
    condicao = (monitor['ResultadoAva'] == 'False') & (monitor['id_op'] == 'AtendeuDistribuido')
    # Atribui '9999||' onde a condição é verdadeira
    monitor.loc[condicao, 'Op Reservada'] = '9996||'

    # Condição para o cálculo da coluna 'Op Reservada'
    condicao = (monitor['Op Reservada'] == '0994||') & (monitor['id_op'] == 'Atendeu') & (
                    monitor['ResultadoAva'] == 'True')
    # Atribui '9999||' onde a condição é verdadeira
    monitor.loc[condicao, 'Op Reservada'] = '9995||'

    # Condição para o cálculo da coluna 'Op Reservada'
    condicao = (monitor['Op Reservada'].str.startswith('9')) & \
                   (monitor['Op Reservada'] != '0994||') & \
                   (monitor['id_op'] == 'Atendeu') & \
                   (monitor['ResultadoAva'] == 'False') & \
                   (monitor['Op Reservada'] != '9995||')

    # Aplicando a condição e substituindo o primeiro '9' por '0'
    monitor.loc[condicao, 'Op Reservada'] = monitor.loc[condicao, 'Op Reservada'].astype(str).str.replace(r'^9',
                                                                                                              '2',
                                                                                                              regex=True)

    monitor = monitor.sort_values(by=['codSitSituacao', 'dataPrevAtualizada', 'Op Reservada', 'Pedido||Prod.||Cor'],
                                      ascending=[True, True, False, True]).reset_index()

    monitor['recalculoData'] = monitor.groupby('codPedido')['Saldo Grade'].cumsum()
    monitor['recalculoData2'] = monitor.groupby('codPedido')['Saldo Grade'].transform('sum')
    monitor['recalculoData'] = (monitor['recalculoData'] / monitor['recalculoData2']).round(2)
    monitor['recalculoData'] = monitor['recalculoData'].astype(str)
    monitor['recalculoData'] = monitor['recalculoData'].str.slice(0, 4)
    monitor['recalculoData'] = monitor['recalculoData'].astype(float)

    # Certifique-se de que 'Entregas Restantes' é do tipo int
    monitor['Entregas Restantes'] = monitor['Entregas Restantes'].astype(int)

    # Condições para a coluna 'entregaAtualizada'
    cond_1 = (monitor['Entregas Restantes'] == 1)
    cond_2_1 = (monitor['Entregas Restantes'] == 2) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
    cond_2_2 = (monitor['Entregas Restantes'] == 2) & (monitor['recalculoData'] * 100 > monitor['ValorMax'])
    cond_3_1 = (monitor['Entregas Restantes'] == 3) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 1)
    cond_3_2 = (monitor['Entregas Restantes'] == 3) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                    monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
    cond_3_3 = (monitor['Entregas Restantes'] == 3) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2)
    cond_4_1 = (monitor['Entregas Restantes'] == 4) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
    cond_4_2 = (monitor['Entregas Restantes'] == 4) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
    cond_4_3 = (monitor['Entregas Restantes'] == 4) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
    cond_4_4 = (monitor['Entregas Restantes'] == 4) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3)

    monitor.loc[cond_1, 'entregaAtualizada'] = 1
    monitor.loc[cond_2_1, 'entregaAtualizada'] = 1
    monitor.loc[cond_2_2, 'entregaAtualizada'] = 2
    monitor.loc[cond_3_1, 'entregaAtualizada'] = 1
    monitor.loc[cond_3_2, 'entregaAtualizada'] = 2
    monitor.loc[cond_3_3, 'entregaAtualizada'] = 3
    monitor.loc[cond_4_1, 'entregaAtualizada'] = 1
    monitor.loc[cond_4_2, 'entregaAtualizada'] = 2
    monitor.loc[cond_4_3, 'entregaAtualizada'] = 3
    monitor.loc[cond_4_4, 'entregaAtualizada'] = 4

    # Caso 5: Entregas Restantes == 5
    cond_5_1 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
    cond_5_2 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
    cond_5_3 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
    cond_5_4 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 4)
    cond_5_5 = (monitor['Entregas Restantes'] == 5) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 4)
    monitor.loc[cond_5_1, 'entregaAtualizada'] = 1
    monitor.loc[cond_5_2, 'entregaAtualizada'] = 2
    monitor.loc[cond_5_3, 'entregaAtualizada'] = 3
    monitor.loc[cond_5_4, 'entregaAtualizada'] = 4
    monitor.loc[cond_5_5, 'entregaAtualizada'] = 5

    # Caso 6: Entregas Restantes == 6
    cond_6_1 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
    cond_6_2 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
    cond_6_3 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
    cond_6_4 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 4)
    cond_6_5 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 4) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 5)
    cond_6_6 = (monitor['Entregas Restantes'] == 6) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 5)
    monitor.loc[cond_6_1, 'entregaAtualizada'] = 1
    monitor.loc[cond_6_2, 'entregaAtualizada'] = 2
    monitor.loc[cond_6_3, 'entregaAtualizada'] = 3
    monitor.loc[cond_6_4, 'entregaAtualizada'] = 4
    monitor.loc[cond_6_5, 'entregaAtualizada'] = 5
    monitor.loc[cond_6_6, 'entregaAtualizada'] = 6

    # Caso 7: Entregas Restantes == 7
    cond_7_1 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
    cond_7_2 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
    cond_7_3 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
    cond_7_4 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 4)
    cond_7_5 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 4) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 5)
    cond_7_6 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 5) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 6)
    cond_7_7 = (monitor['Entregas Restantes'] == 7) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 6)
    monitor.loc[cond_7_1, 'entregaAtualizada'] = 1
    monitor.loc[cond_7_2, 'entregaAtualizada'] = 2
    monitor.loc[cond_7_3, 'entregaAtualizada'] = 3
    monitor.loc[cond_7_4, 'entregaAtualizada'] = 4
    monitor.loc[cond_7_5, 'entregaAtualizada'] = 5
    monitor.loc[cond_7_6, 'entregaAtualizada'] = 6
    monitor.loc[cond_7_7, 'entregaAtualizada'] = 7

    # Caso 8: Entregas Restantes == 8
    cond_8_1 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 <= monitor['ValorMax'])
    cond_8_2 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax']) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 2)
    cond_8_3 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 2) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 3)
    cond_8_4 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 3) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 4)
    cond_8_5 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 4) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 5)
    cond_8_6 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 5) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 6)
    cond_8_7 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 6) & (
                monitor['recalculoData'] * 100 <= monitor['ValorMax'] * 7)
    cond_8_8 = (monitor['Entregas Restantes'] == 8) & (monitor['recalculoData'] * 100 > monitor['ValorMax'] * 7)
    monitor.loc[cond_8_1, 'entregaAtualizada'] = 1
    monitor.loc[cond_8_2, 'entregaAtualizada'] = 2
    monitor.loc[cond_8_3, 'entregaAtualizada'] = 3
    monitor.loc[cond_8_4, 'entregaAtualizada'] = 4
    monitor.loc[cond_8_5, 'entregaAtualizada'] = 5
    monitor.loc[cond_8_6, 'entregaAtualizada'] = 6
    monitor.loc[cond_8_7, 'entregaAtualizada'] = 7
    monitor.loc[cond_8_8, 'entregaAtualizada'] = 8

    monitor['dataPrevAtualizada2'] = (
            monitor['dataPrevAtualizada']
            .str.slice(6, 10) + '-' +
            monitor['dataPrevAtualizada'].str.slice(3, 5) + '-' +
            monitor['dataPrevAtualizada'].str.slice(0, 2)
    )

    # 7 Calculando a nova data de Previsao do pedido
    monitor['dias_a_adicionar2'] = pd.to_timedelta(((monitor['entregaAtualizada'] - 1) * 15),
                                                       unit='d')  # Converte a coluna de inteiros para timedelta
    monitor['dataPrevAtualizada2'] = pd.to_datetime(monitor['dataPrevAtualizada2'], errors='coerce',
                                                        infer_datetime_format=True)
    monitor['dataPrevAtualizada2'].fillna(dataInico, inplace=True)

    monitor['dataPrevAtualizada2'] = monitor['dataPrevAtualizada2'] + monitor['dias_a_adicionar2']


    monitor = monitor.sort_values(by=['dataPrevAtualizada2'], ascending=True)


    monitor.drop(['NecessodadeOP', 'NecessodadeOPAcum', 'id_op', 'Op Reservada'], axis=1, inplace=True)

    # Condição para o cálculo da coluna 'NecessodadeOP'
    condicao = (monitor['Qtd Atende'] > 0)

    # Cálculo da coluna 'NecessodadeOP' de forma vetorizada
    monitor['NecessodadeOP'] = numpy.where(condicao, 0, monitor['QtdSaldo'])
    monitor['NecessodadeOPAcum'] = monitor.groupby('codProduto')['NecessodadeOP'].cumsum()

    monitor['id_op2'] = '-'
    monitor['Op Reservada2'] = '-'

    consultaSql2 = """
    select o.codreduzido as "codProduto", id, "qtdAcumulada" as "qtdAcumulada2", "ocorrencia_sku" from "pcp".ordemprod o where "qtdAcumulada" > 0
    """

    consulta2 = pd.read_sql(consultaSql2, conn)
    for i in range(1, 8):
        consultai = consulta2[consulta2['ocorrencia_sku'] == i]
        x = consultai['codProduto'].count()
        print(f'iteracao {i}: {x}')

        monitor = pd.merge(monitor, consultai, on='codProduto', how='left', suffixes=('', '_consultai'))

        condicao = (monitor['NecessodadeOP'] == 0) | (monitor['NecessodadeOPAcum'] <= monitor['qtdAcumulada2']) | (
                    monitor['id_op2'] == 'Atendeu')
        monitor['id_op2'] = numpy.where(condicao, 'Atendeu', 'nao atendeu')

        monitor['Op Reservada2'] = numpy.where(
            (monitor['id_op2'] == 'Atendeu') & (monitor['NecessodadeOP'] > 0) & (monitor['Op Reservada2'] == '-'),
            monitor['id'].fillna(monitor['Op Reservada2']), monitor['Op Reservada2'])

        monitor.loc[monitor['id_op2'] == 'Atendeu', 'NecessodadeOP'] = 0
        monitor = monitor.drop(['id', 'qtdAcumulada2', 'ocorrencia_sku'], axis=1)


    consulta3 = pd.read_sql("""select id as "Op Reservada2" , numeroop, "codFaseAtual" from "pcp".ordemprod o   """, conn)
    monitor = pd.merge(monitor,consulta3,on='Op Reservada2',how='left')

    #Avaliar se ficou op para tras sem incluir

    descricaoArquivo = dataInico + '_' + dataFim

    monitor.to_csv(f'./dados/monitorOps{descricaoArquivo}.csv')





    monitor1 = monitor[['numeroop','dataPrevAtualizada2','codFaseAtual',"codItemPai","QtdSaldo"]]
    monitor2 = monitor[['numeroop','dataPrevAtualizada2','codFaseAtual',"codItemPai","QtdSaldo","codProduto"]]


    # Converter a coluna 'dataPrevAtualizada2' para string no formato desejado
    monitor1['dataPrevAtualizada2'] = monitor1['dataPrevAtualizada2'].dt.strftime('%Y-%m-%d')

    monitor1['dataPrevAtualizada2'] = pd.to_datetime(monitor1['dataPrevAtualizada2'], errors='coerce',
                                                        infer_datetime_format=True)

    mascara = (monitor1['dataPrevAtualizada2'] >= dataInico) & (monitor1['dataPrevAtualizada2'] <= dataFim)
    monitor1['dataPrevAtualizada2'] = monitor1['dataPrevAtualizada2'].dt.strftime('%Y-%m-%d')

    monitor1['numeroop'].fillna('-',inplace=True)
    monitor1 = monitor1.loc[mascara]

    monitor1 = monitor1[monitor1['numeroop'] != '-']

    monitor1['Ocorrencia Pedidos'] =1
    monitor1 = monitor1.groupby('numeroop').agg({'codFaseAtual':'first','Ocorrencia Pedidos': 'sum',"codItemPai":"first","QtdSaldo":"sum"}).reset_index()
    monitorDetalhadoOps = monitor2.groupby(['numeroop','codProduto']).agg({"QtdSaldo":"sum"}).reset_index()

    monitorDetalhadoOps.to_csv(f'./dados/detalhadoops{descricaoArquivo}.csv')


    monitor1 = monitor1.sort_values(by=['Ocorrencia Pedidos'],
                                      ascending=[False]).reset_index()
    monitor1.rename(columns={'QtdSaldo': 'AtendePçs'}, inplace=True)

    sqlCsw = """Select f.codFase as codFaseAtual , f.nome  from tcp.FasesProducao f WHERE f.codEmpresa = 1"""
    sqlCswPrioridade = """
    SELECT op.numeroOP as numeroop, p.descricao as prioridade, op.dataPrevisaoTermino, e.descricao  FROM TCO.OrdemProd OP 
INNER JOIN tcp.PrioridadeOP p on p.codPrioridadeOP = op.codPrioridadeOP and op.codEmpresa = p.Empresa 
join tcp.engenharia e on e.codempresa = 1 and e.codEngenharia = op.codProduto
WHERE op.situacao = 3 and op.codEmpresa = 1
    """

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sqlCsw)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            get = pd.DataFrame(rows, columns=colunas)
            get['codFaseAtual'] = get['codFaseAtual'].astype(str)
            del rows

            cursor_csw.execute(sqlCswPrioridade)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            get2 = pd.DataFrame(rows, columns=colunas)
            del rows

    monitor1 = pd.merge(monitor1,get,on='codFaseAtual', how='left')
    monitor1 = pd.merge(monitor1,get2,on='numeroop', how='left')
    monitor1.fillna('-',inplace=True)

    dados = {
        '0-Status':True,
        '1-Mensagem': f'Atencao!! Calculado segundo o ultimo monitor emitido',
        '6 -Detalhamento': monitor1.to_dict(orient='records')

    }
    return pd.DataFrame([dados])


def DetalhaOPMonitor(numeroop,dataInico, dataFim):

    sqlCSW = """
    SELECT DISTINCT t.codProduto as itemPai ,t.coditem as codProduto, op.numeroOP as numeroop, op.codTipoOP||'-'||fa.nome as tipoNota, e.descricao ,s.corBase ||'-'|| s.nomeCorBase as cor , ta.descricao, t.qtdePecas1Qualidade as pcsOP  FROM tco.OrdemProd op
inner join tco.OrdemProdTamanhos t on t.codEmpresa = op.codEmpresa and t.numeroOP = op.numeroOP 
inner join tcp.Tamanhos ta on ta.sequencia = t.seqTamanho 
inner join tcp.SortimentosProduto s on s.codEmpresa = op.codEmpresa and s.codProduto = op.codProduto and t.codSortimento = s.codSortimento 
inner join tcp.TipoOP  fa on fa.empresa = 1 and fa.codTipo = op.codTipoOP 
inner join tcp.Engenharia e on e.codengenharia = op.codproduto and e.codempresa = 1
Where op.numeroOP = '""" +numeroop+"""'"""

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sqlCSW)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            sqlCSW = pd.DataFrame(rows, columns=colunas)
            del rows
    descricaoArquivo = dataInico + '_' + dataFim
    monitorDetalhadoOps = pd.read_csv(f'./dados/detalhadoops{descricaoArquivo}.csv')
    monitorDetalhadoOps = monitorDetalhadoOps[monitorDetalhadoOps['numeroop']==numeroop]
    monitorDetalhadoOps['codProduto'] = monitorDetalhadoOps['codProduto'].astype(str)

    monitorDetalhadoOps = pd.merge(monitorDetalhadoOps,sqlCSW,on=['codProduto','numeroop'])

    monitorDetalhadoOps.rename(
        columns={'tipoNota':'01-tipoNota','numeroop':'02-numeroop','codProduto': '03- Cód Reduzido',
                 'cor':'04-cor','descricao':'05-tam','pcsOP':'06-pcsOP','QtdSaldo':'07-Necessidade' },
        inplace=True)

    return monitorDetalhadoOps

def ProdutosSemOP():
    monitorDetalhadoOps = pd.read_csv(f'./dados/monitorOps.csv')
    monitorDetalhadoOps = monitorDetalhadoOps[monitorDetalhadoOps['id_op2'] == 'nao atendeu'].reset_index(drop=True)
    # Filtrando pedidos com 'QtdSaldo' maior que 0
    pedido = monitorDetalhadoOps[monitorDetalhadoOps['QtdSaldo'] > 0].reset_index(drop=True)

    # Selecionando colunas específicas
    colunas_desejadas = ['nomeSKU', 'QtdSaldo', 'codItemPai',  'codProduto', 'codCor']

    # Verifica se todas as colunas desejadas estão presentes no DataFrame 'pedido'
    for coluna in colunas_desejadas:
        if coluna not in pedido.columns:
            raise KeyError(
                f"A coluna '{coluna}' não foi encontrada no DataFrame filtrado. Colunas disponíveis: {pedido.columns.tolist()}")

    df_selecionado = pedido[colunas_desejadas]

    df_selecionado['codItemPai'] = df_selecionado['codItemPai'].astype(str)
    df_selecionado = df_selecionado.groupby('codProduto').agg({'nomeSKU':'first',"codItemPai":"first","QtdSaldo":"sum",'codCor':'first'}).reset_index()

    df_selecionado.rename(
        columns={
                 'codItemPai': '01-codProduto',
                 'nomeSKU': '02-nomeSKU', 'QtdSaldo': '05-QtdSaldoPedido',
                 'codProduto': '03-codReduzido', 'codCor': '04-codCor'}, inplace=True)

    df_selecionado = df_selecionado.sort_values(by=['01-codProduto', '04-codCor'], ascending=True)

    df_selecionado.to_csv('./dados/necessidadeMonitorMP.csv')

    return df_selecionado

def NecessidadeMateriaPrima():
    monitorDetalhadoOps = pd.read_csv('./dados/necessidadeMonitorMP.csv')
