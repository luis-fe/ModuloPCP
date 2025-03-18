import numpy as np
import pytz
from connection import ConexaoPostgreWms
import pandas as pd
from datetime import datetime
from models import FaturamentoClass, FaseClass, ProducaoFases
from models.GestaoOPAberto import FilaFases
from models.GestaoOPAberto.FilaFases import TratamentoInformacaoColecao
from models.Planejamento import itemsPA_Csw, plano, cronograma
from dotenv import load_dotenv, dotenv_values
import os


class MetaFases():

    '''Classe utilizada para construcao das metas por fase a nivel departamental '''
    def __init__(self, codPlano = None, codLote = None):

        self.codPlano = codPlano
        self.codLote = codLote
    def metasFase(self,Codplano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado = False):
        '''Metodo que consulta as meta por fase'''

        # 1 - obtendo o codigo do lote a ser considerado
        nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
        novo = ", ".join(nomes_com_aspas)

        # 1.1 - Abrindo a conexao com o Banco
        conn = ConexaoPostgreWms.conexaoEngine()

        # 2.0 - Verificando se o usuario está analisando em congelamento , CASE NAO:
        if congelado == False:

            # 2.1 sql para pesquisar a previsao a nivel de cor e tam, de acordo com o lote escolhido:

            sqlMetas = """
                SELECT 
                    "codLote", "Empresa", "codEngenharia", "codSeqTamanho", "codSortimento", previsao
                FROM 
                    "PCP".pcp.lote_itens li
                WHERE 
                    "codLote" IN (%s)
            """ % novo

            # 2.2 - Sql que obtem os roteiros das engenharias
            sqlRoteiro = """
                select 
                    * 
                from 
                    "PCP".pcp."Eng_Roteiro" er 
            """

            # 2.3 Sql que obtem a ordem de apresentacao de cada fase
            sqlApresentacao = """
            select 
                "nomeFase" , 
                apresentacao  
            from "PCP".pcp."SeqApresentacao" sa 
            """

            # 2.4 Sql utilizado como apoio para obter a meta a nivel de cor , tamanho, categoria
            consulta = """
            select 
                codigo as "codItem", nome, "unidadeMedida" , "codItemPai" , "codSortimento" as "codSortimento" , "codSeqTamanho" as "codSeqTamanho" , categoria 
            from pcp.itens_csw ic 
            """


            sqlMetas = pd.read_sql(sqlMetas,conn)
            sqlRoteiro = pd.read_sql(sqlRoteiro,conn)
            sqlApresentacao = pd.read_sql(sqlApresentacao,conn)

            consulta = pd.read_sql(consulta, conn)

            # Verificar quais codItemPai começam com '1' ou '2'
            mask = consulta['codItemPai'].str.startswith(('1', '2'))
            # Aplicar as transformações usando a máscara
            consulta['codEngenharia'] = np.where(mask, '0' + consulta['codItemPai'] + '-0', consulta['codItemPai'] + '-0')

            sqlMetas = pd.merge(sqlMetas, consulta, on=["codEngenharia", "codSeqTamanho", "codSortimento"], how='left')
            sqlMetas['codItem'].fillna('-', inplace=True)


            # 3 - Obter o faturamento de um determinado plano e aplicar ao calculo
            faturado = FaturamentoClass.Faturamento(None,None,None,Codplano)
            faturadoPeriodo = faturado.faturamentoPeriodo_Plano()
            faturadoPeriodoPartes = faturado.faturamentoPeriodo_Plano_PartesPeca()
            faturadoPeriodo = pd.concat([faturadoPeriodo, faturadoPeriodoPartes], ignore_index=True)

            sqlMetas = pd.merge(sqlMetas,faturadoPeriodo,on='codItem',how='left')

            # 4 - Aplicando os estoques ao calculo
            #----------------------------------------------------------------------------------------------------------
            estoque = itemsPA_Csw.EstoquePartes()
            sqlMetas = pd.merge(sqlMetas, estoque, on='codItem', how='left')
            #___________________________________________________________________________________________________________

            # 5- Aplicando a carga em producao
            #-------------------------------------------------------------------------------------------------------
            cargaFases = FaseClass.FaseProducao()
            cargas = cargaFases.cargaPartes()

            sqlMetas = pd.merge(sqlMetas, cargas, on='codItem', how='left')

            sqlMetas.fillna({
                'saldo': 0,
                'qtdeFaturada': 0,
                'estoqueAtual': 0,
                'carga': 0
            }, inplace=True)
            #--------------------------------------------------------------------------------------------------------

            # 6 Analisando se esta no periodo de faturamento, e considerando o que falta a entregar de colecoes passadas

            diaAtual = datetime.strptime(self.obterDiaAtual(), '%Y-%m-%d')
            planoAtual = plano.ConsultaPlano()
            planoAtual = planoAtual[planoAtual['codigo'] == Codplano].reset_index()
            IniFat = datetime.strptime(planoAtual['inicoFat'][0], '%Y-%m-%d')


            if diaAtual >= IniFat:
                sqlMetas['FaltaProgramar1'] = sqlMetas['previsao'] - (
                            sqlMetas['estoqueAtual'] + sqlMetas['carga'] + sqlMetas['qtdeFaturada'])

            # 6.2 caso o faturamento da colecao atual nao tenha iniciado
            else:
                sqlMetas['estoque-saldoAnt'] = sqlMetas['estoqueAtual'] - sqlMetas['saldo']
                sqlMetas['FaltaProgramar1'] = sqlMetas['previsao'] - (sqlMetas['estoque-saldoAnt'] + sqlMetas['carga'])

            # 7 - criando a coluna do faltaProgramar , retirando os produtos que tem falta programar negativo
            sqlMetas['FaltaProgramar'] = np.where(sqlMetas['FaltaProgramar1'] > 0, sqlMetas['FaltaProgramar1'], 0)

            # 8 - Salvando os dados para csv que é o retrado da previsao x falta programar a nivel sku
            load_dotenv('db.env')
            caminhoAbsoluto = os.getenv('CAMINHO')
            sqlMetas.to_csv(f'{caminhoAbsoluto}/dados/analise.csv')

            print('excutando a etata 8:Salvando os dados para csv que é o retrado da previsao x falta programar a nivel sku')


            # 9 - Encontrando o falta Programar geral por produto

            Meta = sqlMetas.groupby(["codEngenharia", "codSeqTamanho", "codSortimento", "categoria"]).agg(
                {"previsao": "sum", "FaltaProgramar": "sum"}).reset_index()

            filtro = Meta[Meta['codEngenharia'].str.startswith('0')]
            totalPc = filtro['previsao'].sum()
            totalFaltaProgramar = filtro['FaltaProgramar'].sum()
            novo2 = novo.replace('"', "-")
            Totais = pd.DataFrame(
                [{'0-Previcao Pçs': f'{totalPc} pcs', '01-Falta Programar': f'{totalFaltaProgramar} pçs'}])
            Totais.to_csv(f'{caminhoAbsoluto}/dados/Totais{novo2}.csv')

            # Carregando o Saldo COLECAO ANTERIOR

            Meta = pd.merge(Meta, sqlRoteiro, on='codEngenharia', how='left')
            # Converter as colunas para arrays do NumPy
            codFase_array = Meta['codFase'].values
            codEngenharia_array = Meta['codEngenharia'].values

            # Filtrar as linhas onde 'codFase' é 401
            fase_401 = codFase_array == 401
            fase_426 = codFase_array == 426
            fase_412 = codFase_array == 412
            fase_441 = codFase_array == 441

            # Filtrar as linhas onde 'codEngenharia' não começa com '0'
            nao_comeca_com_0 = np.vectorize(lambda x: not x.startswith('0'))(codEngenharia_array)

            # Combinar as duas condições para filtrar as linhas
            filtro_comb = fase_401 & nao_comeca_com_0
            filtro_comb2 = fase_426 & nao_comeca_com_0
            filtro_comb3 = fase_412 & nao_comeca_com_0
            filtro_comb4 = fase_441 & nao_comeca_com_0

            # Aplicar o filtro invertido
            Meta = Meta[~(filtro_comb | filtro_comb2 | filtro_comb3 | filtro_comb4)]

            Meta.to_csv(f'{caminhoAbsoluto}/dados/analiseFaltaProgrFases.csv')

            Meta = Meta.groupby(["codFase", "nomeFase"]).agg({"previsao": "sum", "FaltaProgramar": "sum"}).reset_index()
            Meta = pd.merge(Meta, sqlApresentacao, on='nomeFase', how='left')
            Meta['apresentacao'] = Meta.apply(lambda x: 0 if x['codFase'] == 401 else x['apresentacao'], axis=1)

            Meta = Meta.sort_values(by=['apresentacao'], ascending=True)  # escolher como deseja classificar

            cronogramaS = cronograma.CronogramaFases(Codplano)
            Meta = pd.merge(Meta, cronogramaS, on='codFase', how='left')

            colecoes = TratamentoInformacaoColecao(arrayCodLoteCsw)

            filaFase = FilaFases.ApresentacaoFila(colecoes)
            filaFase = filaFase.loc[:,
                       ['codFase', 'Carga Atual', 'Fila']]

            Meta = pd.merge(Meta, filaFase, on='codFase', how='left')
            Meta['Carga Atual'].fillna(0, inplace=True)
            Meta['Fila'].fillna(0, inplace=True)
            Meta['Falta Produzir'] = Meta['Carga Atual'] + Meta['Fila'] + Meta['FaltaProgramar']
            Meta['dias'].fillna(1, inplace=True)
            Meta['Meta Dia'] = Meta['Falta Produzir'] / Meta['dias']
            Meta['Meta Dia'] = Meta['Meta Dia'].round(0)

            # Ponto de Congelamento do lote:
            Meta.to_csv(f'{caminhoAbsoluto}/dados/analiseLote{novo2}.csv')

            realizadoPeriodo = ProducaoFases.ProducaoFases(dataMovFaseIni, dataMovFaseFim, '', 0, '1', 100, 100, [6, 8])
            realizado = realizadoPeriodo.RealizadoMediaMovel()
            realizado['codFase'] = realizado['codFase'].astype(int)
            Meta = pd.merge(Meta, realizado, on='codFase', how='left')

            Meta['Realizado'].fillna(0, inplace=True)
            Meta.fillna('-', inplace=True)
            Meta = Meta[Meta['apresentacao'] != '-']

            dados = {
                '0-Previcao Pçs': f'{totalPc} pcs',
                '01-Falta Programar': f'{totalFaltaProgramar} pçs',
                '1-Detalhamento': Meta.to_dict(orient='records')}

            return pd.DataFrame([dados])

        else:
            load_dotenv('db.env')
            caminhoAbsoluto = os.getenv('CAMINHO')
            novo2 = novo.replace('"', "-")
            Meta = pd.read_csv(f'{caminhoAbsoluto}/dados/analiseLote{novo2}.csv')
            Totais = pd.read_csv(f'{caminhoAbsoluto}/dados/Totais{novo2}.csv')
            totalPc = Totais['0-Previcao Pçs'][0]
            totalFaltaProgramar = Totais['01-Falta Programar'][0]

            realizadoPeriodo = ProducaoFases.ProducaoFases(dataMovFaseIni, dataMovFaseFim, '', 0, '1', 100, 100, [6, 8])
            realizado = realizadoPeriodo.RealizadoMediaMovel()
            realizado['codFase'] = realizado['codFase'].astype(int)
            Meta = pd.merge(Meta, realizado, on='codFase', how='left')

            Meta['Realizado'].fillna(0, inplace=True)
            Meta.fillna('-', inplace=True)
            Meta = Meta[Meta['apresentacao'] != '-']

            dados = {
                '0-Previcao Pçs': f'{totalPc} pcs',
                '01-Falta Programar': f'{totalFaltaProgramar} pçs',
                '1-Detalhamento': Meta.to_dict(orient='records')}

            return pd.DataFrame([dados])


    def obterDiaAtual(self):

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return agora


