'''
Modelagem do painel de controle das Ops na fabrica
'''
import re
import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
from datetime import datetime
import datetime
import pytz

# Passo 1: Buscando as OP's em aberto no CSW
def OPemProcesso(empresa, AREA, filtro = '-', filtroDiferente = '', tempo = 9999, limite = 60, classificar = '-', colecaoF = ''):
    filtro = filtro.upper()
    colecao = FiltroColecao(colecaoF)
    print(f'Printando a colecao {colecao}')

    if (filtro == '-' and filtroDiferente == '' and tempo >= limite and colecaoF =='' ) or (filtro == '' and filtroDiferente == '' and tempo >= limite and colecaoF =='')   :
        sqlOpsAberto = """
        SELECT (select pri.descricao  FROM tcp.PrioridadeOP pri WHERE pri.Empresa = 1 and o.codPrioridadeOP = pri.codPrioridadeOP ) as prioridade, dataInicio as startOP, codProduto  , numeroOP , codTipoOP , codFaseAtual as codFase , codSeqRoteiroAtual as seqAtual,
                      codPrioridadeOP, codLote , 
                      (select l.descricao as lote FROM tcl.Lote l WHERE l.codEmpresa = 1 and l.codlote = o.codlote  )as lote,
                      codEmpresa, (SELECT f.nome from tcp.FasesProducao f WHERE f.codempresa = 1 and f.codfase = o.codFaseAtual) as nomeFase, 
                      (select e.descricao from tcp.Engenharia e WHERE e.codempresa = o.codEmpresa and e.codengenharia = o.codProduto) as descricao
                      FROM tco.OrdemProd o 
                       WHERE o.codEmpresa = 1 and o.situacao = 3 
                       """
        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlOpsAberto)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                OP_emAberto = pd.DataFrame(rows, columns=colunas)
                del rows


        ##Excecao Almoxarifado aviamentos
        OP_emAbertoAvimamento = OP_emAberto.copy()  # Criar uma cópia do DataFrame original
        OP_emAberto = OP_emAberto[OP_emAberto['codFase']!='145']
        OP_emAberto1 = OP_emAberto[OP_emAberto['codFase']!='406']
        OP_emAberto = OP_emAberto1[OP_emAberto1['codFase']!='407']

        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(PesquisarSequenciaRoteiro('409'))
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                roteiroSeparacao = pd.DataFrame(rows, columns=colunas)
                del rows

                cursor_csw.execute(PesquisarSequenciaRoteiro('428'))
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                roteiroCDCostura = pd.DataFrame(rows, columns=colunas)
                del rows


        OP_emAbertoAvimamento = pd.merge(OP_emAbertoAvimamento,roteiroSeparacao,on='numeroOP')
        OP_emAbertoAvimamento = pd.merge(OP_emAbertoAvimamento,roteiroCDCostura,on='numeroOP')
        OP_emAbertoAvimamento['seqAtual'] = OP_emAbertoAvimamento['seqAtual'].astype(int)
        OP_emAbertoAvimamento['seq409'] = OP_emAbertoAvimamento['seq409'].astype(int)
        OP_emAbertoAvimamento['seq428'] = OP_emAbertoAvimamento['seq428'].astype(int)
        OP_emAbertoAvimamento = OP_emAbertoAvimamento[(OP_emAbertoAvimamento['seq409'] < OP_emAbertoAvimamento['seqAtual']) &(OP_emAbertoAvimamento['seq428'] >= OP_emAbertoAvimamento['seqAtual']) ].reset_index()
        OP_emAbertoAvimamento['codFase'] = OP_emAbertoAvimamento.apply(lambda row: '145' if row['codFase'] == '145'else '406', axis=1  )
        OP_emAbertoAvimamento['nomeFase'] = OP_emAbertoAvimamento.apply(lambda row: row['nomeFase'] if row['codFase'] == '145'else 'ALMOX. DE AVIAMENTOS', axis=1  )

        dataGeracaoRequisicao = """SELECT r.numeroOP,r.dataBaixa as dataGerReqOP  FROM tco.MovimentacaoOPFase  r
        WHERE r.codEmpresa = 1 and r.codFase = 409 and r.numeroOP in (select numeroOP from tco.OrdemProd op WHERE op.codempresa =1 and op.situacao = 3)
        """

        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(dataGeracaoRequisicao)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                dataGeracaoRequisicao = pd.DataFrame(rows, columns=colunas)
                del rows

                cursor_csw.execute(RequisicoesAbertas())
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                reqAbertas = pd.DataFrame(rows, columns=colunas)
                del rows

        OP_emAbertoAvimamento = pd.merge(OP_emAbertoAvimamento,dataGeracaoRequisicao,on='numeroOP')
        OP_emAbertoAvimamento = pd.merge(OP_emAbertoAvimamento,reqAbertas,on='numeroOP')
        OP_emAberto = pd.concat([OP_emAberto, OP_emAbertoAvimamento], ignore_index=True)

        # Etapa 2 Tratando a informacao da Descricao do Lote para o formato COLECAO
        #################################################################################################
        OP_emAberto['lote'] = OP_emAberto['lote'].astype(str)
        OP_emAberto['lote'].fillna('-', inplace=True)
        OP_emAberto['COLECAO'] = OP_emAberto['lote'].apply(TratamentoInformacaoColecao)
        OP_emAberto['COLECAO'] = OP_emAberto['COLECAO'] + ' ' + OP_emAberto['lote'].apply(extrair_ano)
        OP_emAberto['COLECAO'].fillna('-', inplace=True)

        OP_emAberto['seqAtual'] = OP_emAberto['seqAtual'].astype(str)
        OP_emAberto['codTipoOP'] = OP_emAberto['codTipoOP'].astype(str)
        #################################################################################################
        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(TipoOP())
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                tipoOP = pd.DataFrame(rows, columns=colunas)
                del rows

                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(DataMov_(AREA))
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                DataMov = pd.DataFrame(rows, columns=colunas)
                del rows

                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(OPporTecerceirizado())
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                terceiros = pd.DataFrame(rows, columns=colunas)
                del rows

                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(LocalizarPartesOP())
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                partes = pd.DataFrame(rows, columns=colunas)
                del rows

        # Etapa 4 Trazendo a informacao do tipo
        tipoOP['codTipoOP'] = tipoOP['codTipoOP'].astype(str)

        OP_emAberto = pd.merge(OP_emAberto, tipoOP, on='codTipoOP', how='left')
        OP_emAberto['codTipoOP'] = OP_emAberto['codTipoOP'] + '-' + OP_emAberto['nomeTipoOp']

        DataMov['seqAtual'] = DataMov['seqAtual'].astype(str)

        consulta = pd.merge(OP_emAberto, DataMov, on=['numeroOP', 'seqAtual'], how='left')

        terceiros['codFase'] = terceiros['codFase'].astype(str)

        consulta = pd.merge(consulta, terceiros, on=['numeroOP', 'codFase'], how='left')

        # ETAPA BUSCANDO AS PARTES DA OP MAE
        partes2 = partes.copy()  # Criar uma cópia do DataFrame original
        partes2.rename(columns={'codNatEstoque': 'nova_codNatEstoque'}, inplace=True)
        partes2 = partes2.loc[:, ['nova_codNatEstoque', 'numeroOP']].reset_index()
        partes2 = pd.merge(partes2, partes, on='numeroOP')
        partes2 = partes2[partes2['nova_codNatEstoque'] != partes2['codNatEstoque']].reset_index()
        partes2.drop(['numeroOP'], axis=1, inplace=True)
        partes2.rename(columns={'nova_codNatEstoque': 'numeroOP'}, inplace=True)

        partes = pd.concat([partes, partes2], ignore_index=True)

        partes['nomeParte'] = partes.apply(
            lambda row: NomePartes(row['nomeParte'], 'BORDADO', 'Parte Bordado'), axis=1)
        partes['nomeParte'] = partes.apply(
            lambda row: NomePartes(row['nomeParte'], 'COSTAS', 'Parte SilkCostas'), axis=1)
        partes['nomeParte'] = partes.apply(
            lambda row: NomePartes(row['nomeParte'], 'SILK', 'Parte Silk'), axis=1)
        partes['codNatEstoque'] = partes['nomeParte'] + ':' + partes['codNatEstoque']
        partes.drop('nomeParte', axis=1, inplace=True)
        partes['sitBaixa'] = partes.apply(lambda row: '🟢bx' if row['sitBaixa'] == '2' else '🔴ab.', axis=1)



        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(RequisicoesOPs())
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                requisicoes = pd.DataFrame(rows, columns=colunas)
                del rows
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(RequisicaoOPsPartes())
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                requisicoesPartes = pd.DataFrame(rows, columns=colunas)
                del rows


        requisicoes = pd.concat([requisicoes, requisicoesPartes], ignore_index=True)

        requisicoes['fase'] = requisicoes['fase'].astype(str)
        requisicoes = requisicoes[
            (requisicoes['fase'] == '425') | (requisicoes['fase'] == '415') | (requisicoes['fase'] == '433') | (
                        requisicoes['fase'] == '437')]
        requisicoes['sitBaixa'].fillna('ab.', inplace=True)
        requisicoes['sitBaixa'] = requisicoes.apply(lambda row: '🟢bx' if row['sitBaixa'] == '1' else '🔴ab.', axis=1)
        requisicoes['codNatEstoque'] = requisicoes.apply(lambda row: 'acabamento' if (
                    row['codNatEstoque'] == 1 and (row['fase'] == '433' or row['fase'] == '437')) else row[
            'codNatEstoque'],
                                                         axis=1)
        requisicoes['codNatEstoque'] = requisicoes.apply(
            lambda row: 'avi.' if row['codNatEstoque'] == 1 else row['codNatEstoque'],
            axis=1)
        requisicoes['codNatEstoque'] = requisicoes.apply(
            lambda row: 'golas' if row['codNatEstoque'] == 2 else row['codNatEstoque'],
            axis=1)
        requisicoes['codNatEstoque'] = requisicoes.apply(
            lambda row: 'setor' if row['codNatEstoque'] == 3 else row['codNatEstoque'],
            axis=1)
        requisicoes['numero'] = requisicoes['numero'].astype(str)
        requisicoes['sitBaixa'] = requisicoes['numero'] + requisicoes['sitBaixa']
        # xx Nesse etapa é concatenado os dataframes Requsicao + Partes.

        requisicoes = pd.concat([requisicoes, partes], ignore_index=True)

        # xx Nessa etapa é excluida as colunas "fase" e "numero" para dar uma limpada no dataframe, deixando mais limpo.
        requisicoes.drop(['fase', 'numero'], axis=1, inplace=True)

        # Agrupando e criando a coluna 'detalhado'
        requisicoes = requisicoes.groupby(['numeroOP']).apply(
            lambda x: ', '.join(f"{codNatEstoque}{sitBaixa}" for codNatEstoque, sitBaixa in
                                zip(x['codNatEstoque'], x['sitBaixa']))).reset_index(
            name='detalhado')

        requisicoes['estaPendente'] = requisicoes.apply(lambda row: substituir_bx(row['detalhado']), axis=1)
        requisicoes['estaPendente'] = requisicoes['estaPendente'].str.replace('🔴ab.', '')
        # Dividir a string em partes usando a vírgula como delimitador
        requisicoes['estaPendente'] = requisicoes.apply(lambda row: row['estaPendente'].split(','), axis=1)
        requisicoes['estaPendente'] = requisicoes.apply(lambda row: list(filter(bool, row['estaPendente'])), axis=1)

        requisicoes['Status Aguardando Partes'] = requisicoes.apply(
            lambda row: f'PENDENTE' if 'ab.' in row["detalhado"] else
            f'OK', axis=1)

        requisicoes['replicar'] = 'replicar'
        consulta['replicar'] = consulta.apply(
            lambda row: 'replicar' if row['codFase'] in ['425', '426', '406', '410', '413', '414', '435', '415',
                                                         '145'] else '-', axis=1)

        consulta = pd.merge(consulta, requisicoes, on=['numeroOP', 'replicar'], how='left')

        # Função para remover os valores do array se começarem com "acabamento"
        def remove_acabamento_from_array(arr):
            if isinstance(arr, list):  # Verifica se arr é uma lista
                return [item if not item.startswith('acabamento') else '' for item in arr]
            else:
                return arr  # Retorna arr inalterado se não for uma lista
        # Aplicando a função à coluna detalhado apenas se cofFase não for '406'
        consulta['estaPendente'] = consulta.apply(
            lambda row: remove_acabamento_from_array(row['estaPendente']) if row['codFase'] != '406' else row['detalhado'],
            axis=1)


        consulta['Status Aguardando Partes'] = consulta.apply(lambda row: f'OK' if row["estaPendente"] == [''] else
                                                     row['Status Aguardando Partes'] , axis=1)


        justificativa = """SELECT CONVERT(varchar(12), codop) as numeroOP, codfase as codFase, textolinha as justificativa1 FROM tco.ObservacoesGiroFasesTexto  t 
                        having empresa = 1 and textolinha is not null"""

        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(justificativa)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                justificativa = pd.DataFrame(rows, columns=colunas)
                del rows



        justificativa = justificativa.groupby(['numeroOP','codFase'])['justificativa1'].apply(' '.join).reset_index()

        justificativa['codFase'] = justificativa['codFase'].astype(str)
        conn2 = ConexaoPostgreWms.conexaoEngine()
        justificativa2 = pd.read_sql('select ordemprod as "numeroOP", fase as "codFase", justificativa as justificativa2 from "PCP".pcp.justificativa ',conn2)
        leadTime2 = pd.read_sql('select categoria, codfase as "codFase", leadtime as meta2, limite_atencao from "PCP".pcp.leadtime_categorias ',conn2)

        pcs = pd.read_sql(
            'select numeroop as "numeroOP", total_pcs as "Qtd Pcs" from pcp.ordemprod ',
            conn2)
        pcs['Qtd Pcs'].fillna(0, inplace=True)
        pcs['Qtd Pcs'] =pcs['Qtd Pcs'] .astype(int)
        pcs= pcs.groupby(['numeroOP']).agg({
            'Qtd Pcs':'sum'
        }).reset_index()

        # Concatenar os DataFrames
        consulta = pd.merge(consulta, justificativa2, on=['numeroOP', 'codFase'], how='left')
        consulta['justificativa2'].fillna('-', inplace=True)

        consulta = pd.merge(consulta, justificativa, on=['numeroOP', 'codFase'], how='left')
        consulta['justificativa1'].fillna('-', inplace=True)

        consulta['justificativa'] = consulta.apply(
            lambda row: row['justificativa2'] if row['justificativa2'] != '-' else row['justificativa1'], axis=1)

        sqlCswLeadTime = """SELECT f.codFase , f.leadTime as meta  FROM tcp.FasesProducao f WHERE f.codEmpresa = 1"""

        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlCswLeadTime)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                leadTime = pd.DataFrame(rows, columns=colunas)
                del rows

                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(DeParaFilhoPaiCategoria())
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                deparaPartes = pd.DataFrame(rows, columns=colunas)
                del rows

        consulta['codFase'] = consulta['codFase'].astype(str)
        leadTime['codFase'] = leadTime['codFase'].astype(str)

        consulta = pd.merge(consulta, deparaPartes, on='codProduto', how='left')
        consulta['descricaoPai'].fillna('-', inplace=True)
        consulta['descricao'] = consulta.apply(
            lambda row: row['descricao'] if row['descricaoPai'] == '-' else row['descricaoPai'], axis=1)

        pcs.fillna(0, inplace=True)

        consulta = pd.merge(consulta, pcs, on='numeroOP', how='left')
        responsabilidade = ResponsabilidadeFases()
        consulta = pd.merge(consulta, responsabilidade, on='codFase', how='left')
        consulta = ExecoesResponsalFases(consulta, '412', '4-PARTE KIT/CONJ')

        consulta = pd.merge(consulta, leadTime, on='codFase', how='left')

        consulta['data_entrada'].fillna('-', inplace=True)
        consulta['data_entrada'] = consulta.apply(
            lambda row: row['dataGerReqOP'] if row['codFase'] == '406' else row['data_entrada'], axis=1)

        consulta['data_entrada'] = consulta.apply(
            lambda row: row['startOP'] if row['data_entrada'] == '-' else row['data_entrada'], axis=1)
        consulta = consulta[consulta['data_entrada'] != '-']
        consulta["prioridade"].fillna('NORMAL', inplace=True)
        consulta.fillna('-', inplace=True)

        consulta['data_entrada'] = consulta['data_entrada'].str.slice(0, 10)
        consulta['data_entrada'] = pd.to_datetime(consulta['data_entrada'], errors='coerce')
        # Verificando e lidando com valores nulos
        hora_str = obterHoraAtual()
        consulta['hora_str'] = hora_str
        consulta['hora_str'] = pd.to_datetime(consulta['hora_str'], errors='coerce')

        consulta['dias na Fase'] = (consulta['hora_str'] - consulta['data_entrada']).dt.days.fillna('')
        consulta['data_entrada'] = consulta['data_entrada'].astype(str)

        consulta.drop('hora_str', axis=1, inplace=True)
        consulta = consulta[consulta['dias na Fase'] != '']

        consulta['Area'] = consulta.apply(lambda row: 'PILOTO' if row['codTipoOP'] == '13-PILOTO' else 'PRODUCAO',
                                          axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '3-CLAUDINO' if row['prioridade'] == 'CLAUDINO' else row['prioridade'], axis=1)

        consulta['prioridade'] = consulta.apply(
            lambda row: '09-URGENTE' if row['prioridade'] == 'URGENTE' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '2-FAT ATRASADO' if row['prioridade'] == 'FAT ATRASADO' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '1-P/FAT.' if row['prioridade'] == 'P/FATURAMENTO' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '1-P/FAT.' if 'FATURAMENTO' in row['prioridade'] else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '08-QP1' if row['prioridade'] == 'QP1' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '08-QM1' if row['prioridade'] == 'QM1' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '07-QP2' if row['prioridade'] == 'QP2' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '07-QM2' if row['prioridade'] == 'QM2' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '06-QP3' if row['prioridade'] == 'QP3' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '06-QM3' if row['prioridade'] == 'QM3' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '05-QP4' if row['prioridade'] == 'QP4' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '05-QM4' if row['prioridade'] == 'QM4' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '04-QP5' if row['prioridade'] == 'QP5' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '04-QM5' if row['prioridade'] == 'QM5' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '03-QP6' if row['prioridade'] == 'QP6' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '03-QM6' if row['prioridade'] == 'QM6' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '02-QP7' if row['prioridade'] == 'QP7' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '02-QM7' if row['prioridade'] == 'QM7' else row['prioridade'], axis=1)
        consulta['prioridade'] = consulta.apply(
            lambda row: '01-NORMAL' if row['prioridade'] == 'NORMAL' else row['prioridade'], axis=1)

        consulta['prioridade'] = consulta.apply(
            lambda row: '01-' + row['prioridade'] if '-' not in row['prioridade'] else row['prioridade'], axis=1)

        consulta['categoria'] = '-'

        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('CAMISA', row['descricao'], 'CAMISA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('POLO', row['descricao'], 'POLO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('BATA', row['descricao'], 'CAMISA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('TRICOT', row['descricao'], 'TRICOT', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('BONE', row['descricao'], 'BONE', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('CARTEIRA', row['descricao'], 'CARTEIRA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('TSHIRT', row['descricao'], 'CAMISETA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('REGATA', row['descricao'], 'CAMISETA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('BLUSAO', row['descricao'], 'AGASALHOS', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('BABY', row['descricao'], 'CAMISETA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('JAQUETA', row['descricao'], 'JAQUETA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('CARTEIRA', row['descricao'], 'CARTEIRA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('BONE', row['descricao'], 'BONE', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('CINTO', row['descricao'], 'CINTO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('PORTA CAR', row['descricao'], 'CARTEIRA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('CUECA', row['descricao'], 'CUECA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('MEIA', row['descricao'], 'MEIA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('SUNGA', row['descricao'], 'SUNGA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: Categoria('SHORT', row['descricao'], 'SHORT', row['categoria']), axis=1)

        consulta['nome'] = consulta.apply(
            lambda row: ApelidoFaccionista(row['nome'], 'CLAUDIANA', '(CLAUDIANA)'), axis=1)

        consulta['nome'] = consulta.apply(
            lambda row: ApelidoFaccionista(row['nome'], 'LPS', '( LPS)'), axis=1)
        consulta['nome'] = consulta.apply(
            lambda row: ApelidoFaccionista(row['nome'], 'BELLA D', '( DEVANI)'), axis=1)
        consulta['nome'] = consulta.apply(
            lambda row: ApelidoFaccionista(row['nome'], 'PATRICIO', '(PATRICIO)'), axis=1)
        consulta['nome'] = consulta.apply(
            lambda row: ApelidoFaccionista(row['nome'], 'PRODUZIR', '(PRODUZIR)'), axis=1)

        consulta['nome'] = consulta['nome'].replace('(-)', '')
        consulta['nomeFase'] = consulta['nomeFase'] + ' ' + consulta['nome']

        consulta = pd.merge(consulta, leadTime2, on=['codFase', 'categoria'], how='left')

        ### Corrgindo as colunas para aceitar valores inteiros
        consulta['meta2'].fillna(0, inplace=True)
        consulta['meta2'] = consulta['meta2'].astype(int)

        consulta['meta'].fillna(0, inplace=True)
        consulta['meta'] = consulta['meta'].replace('-','0')
        consulta['meta'] = consulta['meta'].astype(int)

        consulta['limite_atencao'].fillna(0, inplace=True)
        consulta['limite_atencao'] = consulta['limite_atencao'].astype(int)

        consulta['dias na Fase'] = consulta['dias na Fase'].astype(int)
        ## Fim dessa etapa

        consulta['meta'] = consulta.apply(lambda row: row['meta'] if row['meta2'] == 0 else row['meta2'], axis=1)
        consulta.drop('meta2', axis=1, inplace=True)

        consulta['status'] = consulta.apply(
            lambda row: '2-Atrasado' if row['dias na Fase'] > row['meta'] else '0-Normal', axis=1)
        consulta['status'] = consulta.apply(
            lambda row: '1-Atencao' if row['status'] == '2-Atrasado' and row['dias na Fase'] < row[
                'limite_atencao'] else row['status'], axis=1)

        if classificar == 'tempo':
            consulta = consulta.sort_values(by=['dias na Fase'], ascending=False)  # escolher como deseja classificar

        elif classificar == 'status':
            consulta = consulta.sort_values(by=['status', 'dias na Fase'],
                                            ascending=False)  # escolher como deseja classificar

        elif classificar == 'prioridade':
            print('deu certo: gerado ')
            consulta = consulta.sort_values(by=['prioridade', 'status', 'dias na Fase'],
                                            ascending=False)  # escolher como deseja classificar

        else:
            consulta = consulta

        consulta['filtro'] = consulta['prioridade'] + consulta['codProduto'] + consulta['categoria'] + consulta[
            'codFase'] + '-' + consulta['nomeFase'] + consulta['numeroOP'] + consulta['responsavel'] + consulta[
                                 'status']
        consulta['filtro'] = consulta['filtro'].str.replace(' ', '')
        consulta['filtro'] = consulta['filtro'].str.replace('-001', '')
        consulta['filtro'] = consulta['filtro'].str.replace('-2-Atrasado', 'ATRASADO')

        consulta.drop(['justificativa2', 'justificativa1', 'seqRoteiro', 'seqAtual', 'nomeTipoOp', 'replicar'], axis=1,
                      inplace=True)

        consulta = consulta[(consulta['codFase'] != '426') | (consulta['codTipoOP'] != '2-PARTE DE PECA')]

        consulta.to_csv('./dados/cargaOP.csv',index=True)
        # Retirar o "-" da prioridade :
        consulta['prioridade'] = consulta['prioridade'].str.split('-').str[1]

        consulta = consulta[consulta['Area'] == AREA]

        consulta.drop('filtro', axis=1, inplace=True)

        consulta['Qtd Pcs'] = consulta['Qtd Pcs'].replace('-', 0)
        QtdPcs = consulta['Qtd Pcs'].sum()
        QtdPcs = "{:,.0f}".format(QtdPcs)
        QtdPcs = QtdPcs.replace(',', '')

        totalOP = consulta['numeroOP'].count()
        totalOP = "{:,.0f}".format(totalOP)
        totalOP = totalOP.replace(',', '')

        Atrazado = consulta[consulta['status'] == '2-Atrasado']
        totalAtraso = Atrazado['numeroOP'].count()
        totalAtraso = "{:,.0f}".format(totalAtraso)
        totalAtraso = totalAtraso.replace(',', '')

        Atencao = consulta[consulta['status'] == '1-Atencao']
        totalAtencao = Atencao['numeroOP'].count()
        totalAtencao = "{:,.0f}".format(totalAtencao)
        totalAtencao = totalAtencao.replace(',', '')

        consulta = consulta.head(500)
        print('carregando tudo')

        dados = {
            '000-StatusCongelado': False,
            '0-Total DE pçs': f'{QtdPcs} Pçs',
            '1-Total OP': f'{totalOP} Ops',
            '2- OPs Atrasadas': f'{totalAtraso} Ops',
            '2.1- OPs Atencao': f'{totalAtencao} Ops',
            '3 -Detalhamento': consulta.to_dict(orient='records')

        }

        return pd.DataFrame([dados])

    elif filtro == '-' or filtro == '':

        consulta = pd.read_csv('./dados/cargaOP.csv')
        consulta.fillna('-', inplace=True)

        if isinstance(colecao, pd.DataFrame):
            consulta = pd.merge(consulta,colecao,on='COLECAO')
        ##Converter string para lista
        #consulta['estaPendente'] = consulta['estaPendente'].apply(ast.literal_eval)
        consulta['estaPendente'] = consulta['estaPendente'].str.replace("[","")
        consulta['estaPendente'] = consulta['estaPendente'].str.replace("]", "")
        consulta['estaPendente'] = consulta.apply(lambda row: row['estaPendente'].split(','), axis=1)

        consulta = consulta[consulta['Area'] == AREA]
        if classificar == 'tempo':
            consulta = consulta.sort_values(by=['dias na Fase'], ascending=False)  # escolher como deseja classificar

        elif classificar == 'status':
            consulta = consulta.sort_values(by=['status','dias na Fase'], ascending=False)  # escolher como deseja classificar

        elif classificar == 'prioridade':
            print('deu certo: buscou do que ta salvo')
            consulta = consulta.sort_values(by=['prioridade','status','dias na Fase'], ascending=False)  # escolher como deseja classificar

        else:
            consulta= consulta


        consulta.drop('filtro', axis=1, inplace=True)

        consulta['Qtd Pcs'] = consulta['Qtd Pcs'].replace('-', 0)
        consulta['Qtd Pcs'].fillna(0, inplace=True)
        consulta['Qtd Pcs'] = consulta['Qtd Pcs'].astype(float)
        # Retirar o "-" da prioridade :
        consulta['prioridade'] = consulta['prioridade'].str.split('-').str[1]

        QtdPcs = consulta['Qtd Pcs'].sum()

        QtdPcs = "{:,.0f}".format(QtdPcs)
        QtdPcs = QtdPcs.replace(',', '')

        totalOP = consulta['numeroOP'].count()
        totalOP = "{:,.0f}".format(totalOP)
        totalOP = totalOP.replace(',', '')

        Atrazado = consulta[consulta['status'] == '2-Atrasado']
        totalAtraso = Atrazado['numeroOP'].count()
        totalAtraso = "{:,.0f}".format(totalAtraso)
        totalAtraso = totalAtraso.replace(',', '')

        Atencao = consulta[consulta['status'] == '1-Atencao']
        totalAtencao = Atencao['numeroOP'].count()
        totalAtencao = "{:,.0f}".format(totalAtencao)
        totalAtencao = totalAtencao.replace(',', '')

        dados = {
            '0-Total DE pçs': f'{QtdPcs} Pçs',
            '1-Total OP': f'{totalOP} Ops',
            '2- OPs Atrasadas': f'{totalAtraso} Ops',
            '2.1- OPs Atencao': f'{totalAtencao} Ops',
            '3 -Detalhamento': consulta.to_dict(orient='records')

        }
        return pd.DataFrame([dados])

    #essa etapa busca do que ta salvo
    else:
        filtros = pd.read_csv('./dados/cargaOP.csv')
        filtros = filtros[filtros['Area'] == AREA]
        if isinstance(colecao, pd.DataFrame):
            filtros = pd.merge(filtros, colecao, on='COLECAO')

        # Dividir a string em partes usando a vírgula como delimitador
        filtros['estaPendente'] = filtros['estaPendente'].str.replace("[", "")
        filtros['estaPendente'] = filtros['estaPendente'].str.replace("]", "")
        filtros['estaPendente'] = filtros['estaPendente'].str.replace("'", "")

        filtros['estaPendente'] = filtros.apply(lambda row: row['estaPendente'].split(','), axis=1)

        ### ETAPA X : PROCEDIMENTO DE VARREDURA DOS TIPOS DE FILTRO INFORMADO PELO USUARIO:
        ###     NIVEL 1: NAO FILTRA O DATAFRAME, NIVEL2: FILTRA O DATAFRAME:

        matrizNivel = filtro.split("/")
        nivel1Array = []
        nivel2Array = []
        for busca in matrizNivel:
                nivel = ReconhecerFiltro(busca)

                if nivel == 'N2':
                    nivel2Array.append(busca)
                else:
                    nivel1Array.append(busca)

        ## Verifica se existe algum filtro de nivel2 para fazer uma filtragem nos dados
        if len(nivel2Array) == 0:
                print("sem filtro de nivel 2")
        else:
                filtrarValor = nivel2Array[0]
                print(f'filtro de nivel 2 encontrado: {filtrarValor}')
                filtros = filtros[filtros['filtro'].str.contains(filtrarValor)]

        ### FIM ETAPA X.

        if filtroDiferente == '':
                print(nivel1Array)

                filtrosNovo = None
                contador = 0

                for i in nivel1Array:
                    contador = 1 + contador

                    filtrosNovoCadeia = filtros[filtros['filtro'].str.contains(i)]
                    if i == 1:
                        filtrosNovo = filtrosNovoCadeia
                    else:

                        filtrosNovo = pd.concat([filtrosNovo, filtrosNovoCadeia], ignore_index=True)

        else:
                print(nivel1Array)
                filtroDif = filtros[~filtros['filtro'].str.contains(filtroDiferente)]

                filtrosNovo = None
                contador = 0
                for i in nivel1Array:
                    contador = 1 + contador

                    filtrosNovoCadeia = filtroDif[filtroDif['filtro'].str.contains(i)]
                    if i == 1:
                        filtrosNovo = filtrosNovoCadeia
                    else:

                        filtrosNovo = pd.concat([filtrosNovo, filtrosNovoCadeia], ignore_index=True)

        if classificar == 'tempo':
                filtrosNovo = filtrosNovo.sort_values(by=['dias na Fase'],
                                                      ascending=False)  # escolher como deseja classificar

        elif classificar == 'status':
                filtrosNovo = filtrosNovo.sort_values(by=['status', 'dias na Fase'],
                                                      ascending=False)  # escolher como deseja classificar

        elif classificar == 'prioridade':
                print('deu certo: buscou do que ta salvo')
                filtrosNovo = filtrosNovo.sort_values(by=['prioridade', 'status', 'dias na Fase'],
                                                      ascending=False)  # escolher como deseja classificar

        else:
                filtrosNovo = filtrosNovo

        if filtrosNovo.empty:
                dados = {
                    '0-Total DE pçs': '',
                    '1-Total OP': '',
                    '2- OPs Atrasadas': '',
                    '2.1- OPs Atencao': '',
                    '3 -Detalhamento': ''

                }

                return pd.DataFrame([dados])

        else:
                print('bateu aqui')
                filtrosNovo['Qtd Pcs'] = pd.to_numeric(filtrosNovo['Qtd Pcs'], errors='coerce').fillna(0).astype(int)
                QtdPcs = filtrosNovo['Qtd Pcs'].sum()

                QtdPcs = str(QtdPcs).replace(',', '')

                totalOP = filtrosNovo['numeroOP'].count()
                totalOP = "{:,.0f}".format(totalOP)
                totalOP = totalOP.replace(',', '')

                Atrazado = filtrosNovo[filtrosNovo['status'] == '2-Atrasado']
                totalAtraso = Atrazado['numeroOP'].count()
                totalAtraso = "{:,.0f}".format(totalAtraso)
                totalAtraso = totalAtraso.replace(',', '')

                Atencao = filtrosNovo[filtrosNovo['status'] == '1-Atencao']
                totalAtencao = Atencao['numeroOP'].count()
                totalAtencao = "{:,.0f}".format(totalAtencao)
                totalAtencao = totalAtencao.replace(',', '')

                filtrosNovo.fillna('-', inplace=True)

                filtrosNovo.drop(['filtro', 'Unnamed: 0'], axis=1, inplace=True)
                # Retirar o "-" da prioridade :
                filtrosNovo['prioridade'] = filtrosNovo['prioridade'].str.split('-').str[1]

                dados = {
                    '0-Total DE pçs': f'{QtdPcs} Pçs',
                    '1-Total OP': f'{totalOP} Ops',
                    '2- OPs Atrasadas': f'{totalAtraso} Ops',
                    '2.1- OPs Atencao': f'{totalAtencao} Ops',
                    '3 -Detalhamento': filtrosNovo.to_dict(orient='records')

                }

                return pd.DataFrame([dados])









def DistinctColecao():
    sqlOpsAberto = """
    SELECT (select pri.descricao  FROM tcp.PrioridadeOP pri WHERE pri.Empresa = 1 and o.codPrioridadeOP = pri.codPrioridadeOP ) as prioridade, dataInicio as startOP, codProduto  , numeroOP , codTipoOP , codFaseAtual as codFase , codSeqRoteiroAtual as seqAtual,
                  codPrioridadeOP, codLote , 
                  (select l.descricao as lote FROM tcl.Lote l WHERE l.codEmpresa = 1 and l.codlote = o.codlote  )as lote,
                  codEmpresa, (SELECT f.nome from tcp.FasesProducao f WHERE f.codempresa = 1 and f.codfase = o.codFaseAtual) as nomeFase, 
                  (select e.descricao from tcp.Engenharia e WHERE e.codempresa = o.codEmpresa and e.codengenharia = o.codProduto) as descricao
                  FROM tco.OrdemProd o 
                   WHERE o.codEmpresa = 1 and o.situacao = 3 
                   """
    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sqlOpsAberto)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            OP_emAberto = pd.DataFrame(rows, columns=colunas)
            del rows



    # Etapa Tratando a informacao da Descricao do Lote para o formato COLECAO
    OP_emAberto['lote'] = OP_emAberto['lote'].astype(str)
    OP_emAberto['lote'].fillna('-', inplace=True)
    OP_emAberto['COLECAO'] = OP_emAberto['lote'].apply(TratamentoInformacaoColecao)
    OP_emAberto['COLECAO'] = OP_emAberto['COLECAO'] + ' ' + OP_emAberto['lote'].apply(extrair_ano)
    OP_emAberto['COLECAO'].fillna('-', inplace=True)
    OP_emAberto = OP_emAberto.loc[:,['COLECAO']]
    OP_emAberto = OP_emAberto.drop_duplicates()

    return OP_emAberto


def FiltroColecao(colecao):
    # Transformando o array em um dataFrame
    if colecao == '' or colecao == '-':
        return '-'
    else:
        df = pd.DataFrame(colecao, columns=['COLECAO'])
        df['filtrado'] = True
        #Carregando o distinct
        distinct  = DistinctColecao()
        distinct = pd.merge(distinct, df, on='COLECAO',how='left')

        contagemDistinct = distinct['COLECAO'].count()

        distinct2 = distinct[distinct['filtrado'] == True]
        contagemDistinct2 = distinct2['COLECAO'].count()

        if contagemDistinct == contagemDistinct2:
            return '-'
        else:
            return distinct2
def TratamentoInformacaoColecao(descricaoLote):
    if 'INVERNO' in descricaoLote:
        return 'INVERNO'
    elif 'PRI' in descricaoLote:
        return 'VERAO'
    elif 'ALT' in descricaoLote:
        return 'ALTO VERAO'
    elif 'VER' in descricaoLote:
        return 'VERAO'
    else:
        return 'ENCOMENDAS'


def extrair_ano(descricaoLote):
    match = re.search(r'\b2\d{3}\b', descricaoLote)
    if match:
        return match.group(0)
    else:
        return None


def PesquisarSequenciaRoteiro(codfase):
            consulta = """
            SELECT r.numeroOP , r.codSeqRoteiro as seq""" + codfase + """ FROM tco.RoteiroOP r
        WHERE r.codEmpresa = 1 and r.codFase = """ \
                       + codfase + \
                       """ and r.numeroOP in (select numeroOP from tco.OrdemProd op WHERE op.codempresa =1 and op.situacao = 3)
                       """
            return consulta


def RequisicoesAbertas():
    consulta = """
    SELECT DISTINCT r.numOPConfec as numeroOP   from tcq.Requisicao r
WHERE r.codEmpresa = 1 and r.numOPConfec in (select numeroOP from tco.OrdemProd op WHERE op.codempresa =1 and op.situacao = 3)
and sitBaixa <> 1 and r.seqRoteiro <> 408 
    """
    return consulta


def TipoOP():

    TipoOP = 'SELECT t.codTipo as codTipoOP, t.nome as nomeTipoOp  FROM tcp.TipoOP t WHERE t.Empresa = 1'

    return TipoOP


def DataMov_(AREA):

    if AREA == 'PRODUCAO':
        DataMov = 'SELECT numeroOP, dataMov as data_entrada , horaMov , seqRoteiro, (seqRoteiro + 1) as seqAtual FROM tco.MovimentacaoOPFase mf '\
            ' WHERE  numeroOP in (SELECT o.numeroOP from  tco.OrdemProd o' \
            ' having o.codEmpresa = 1 and o.situacao = 3 and o.codtipoop <> 13) and mf.codempresa = 1 order by codlote desc'
    else:
        DataMov = 'SELECT numeroOP, dataMov as data_entrada , horaMov , seqRoteiro, (seqRoteiro + 1) as seqAtual FROM tco.MovimentacaoOPFase mf '\
            ' WHERE  numeroOP in (SELECT o.numeroOP from  tco.OrdemProd o' \
            ' having o.codEmpresa = 1 and o.situacao = 3 and o.codtipoop = 13) and mf.codempresa = 1 order by codlote desc'

    return DataMov

#SQL DE BUSCA DE TERCEIRIZADOS POR OP E FASE - Velocidade Média: 0,700 s

def OPporTecerceirizado():
    OpTercerizados = 'SELECT CONVERT(VARCHAR(10), R.codOP) AS numeroOP, R.codFase as codFase, R.codFac,'\
  ' (SELECT nome  FROM tcg.Faccionista  f WHERE f.empresa = 1 and f.codfaccionista = r.codfac) as nome'\
 ' FROM TCT.RemessaOPsDistribuicao R'\
' INNER JOIN tco.OrdemProd op on'\
    ' op.codempresa = r.empresa and op.numeroop = CONVERT(VARCHAR(10), R.codOP)'\
    ' WHERE R.Empresa = 1 and op.situacao = 3 and r.situac = 2'

    return OpTercerizados

#SQL DE BUSCA DAS PARTES DAS OPS : velocidade Média : 0,35 segundos (OTIMO)

def LocalizarPartesOP():

    partes = """
    SELECT p.codlote as numero, codopconjunto as numeroOP , '425' as fase, op.situacao as sitBaixa, codOPParte as codNatEstoque,
              (SELECT e.descricao from tcp.Engenharia e WHERE e.codempresa = 1 and e.codengenharia = op.codProduto) as nomeParte
              FROM tco.RelacaoOPsConjuntoPartes p
              inner join tco.OrdemProd op on op.codEmpresa = p.Empresa and op.numeroOP = p.codOPParte 
              WHERE codopconjunto in (SELECT op.numeroop from tco.OrdemProd op WHERE op.codempresa = 1 and op.situacao = 3 and op.codfaseatual = 426 )
              """

    return partes

def NomePartes(entrada, referencia, saida):
    if referencia in entrada:
        return saida
    else:
        return entrada


#SQL DE BUSCA DAS REQUISICOES DAS OPS : velocidade Média : 1,20 segundos

def RequisicoesOPs():

    requisicoes = """
    SELECT numero,numOPConfec as numeroOP ,  seqRoteiro as fase, sitBaixa, codNatEstoque
                  FROM tcq.Requisicao r WHERE r.codEmpresa = 1 and
                  r.numOPConfec in (SELECT op.numeroop from tco.OrdemProd op WHERE op.codempresa = 1 and op.situacao = 3)
    """

    return requisicoes

def RequisicaoOPsPartes():
    requisicao = """
        SELECT numero,codOPParte as numeroOP  ,  seqRoteiro as fase, sitBaixa, codNatEstoque
                  FROM tcq.Requisicao r 
                  inner join tco.RelacaoOPsConjuntoPartes p on p.codOPConjunto = r.numOPConfec 
                  WHERE r.codEmpresa = 1 and
                  r.numOPConfec in (SELECT op.numeroop from tco.OrdemProd op WHERE op.codempresa = 1 and op.situacao = 3)
    """
    return requisicao

def substituir_bx(conjunto):
    partes = [parte.strip() for parte in conjunto.split(',')]
    partes = ['' if 'bx' in parte else parte for parte in partes]
    return ','.join(partes)


#SQL DEPARA DA ENGENHARIA PAI X FILHO: velocidade Média : 0,20 segundos

def DeParaFilhoPaiCategoria():

    dePara = "SELECT e.codEngenharia as codProduto,"\
     " (SELECT ep.descricao from tcp.Engenharia ep WHERE ep.codempresa = 1 and ep.codengenharia like '%-0' and '01'||SUBSTRING(e.codEngenharia, 3,9) = ep.codEngenharia) as descricaoPai"\
" FROM tcp.Engenharia e"\
" WHERE e.codEmpresa = 1 and e.codEngenharia like '6%' and e.codEngenharia like '%-0' and e.codEngenharia not like '65%'"

    return dePara

def ResponsabilidadeFases():
    conn = ConexaoPostgreWms.conexaoEngine()

    retorno = pd.read_sql('SELECT x.* FROM pcp."responsabilidadeFase" x ',conn)

    return retorno


def ExecoesResponsalFases(dataframe, fase, tipoop):
    dataframe['responsavel'] = dataframe.apply(lambda row: '' if row['codFase'] == fase and row['codTipoOP'] == tipoop else row['responsavel'], axis=1 )
    return dataframe

def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.datetime.now(fuso_horario)
    hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
    return hora_str

def Categoria(contem, valorReferencia, valorNovo, categoria):
    if contem in valorReferencia:
        return valorNovo
    else:
        return categoria


def ApelidoFaccionista(entrada, referencia, saida):
    if referencia in entrada:
        return saida
    else:
        return entrada[0:9]

def ReconhecerFiltro(filtro):
    palavras_chave = ['CAMI', 'CALCA', 'SHORT', 'BONE', 'POLO','JAQUET','ATRASA']

    if any(palavra in filtro for palavra in palavras_chave):
        return "N2"
    else:
        return 'N1'