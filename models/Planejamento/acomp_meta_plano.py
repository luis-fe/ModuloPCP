import pandas as pd
from connection import ConexaoPostgreWms
from models.Planejamento import SaldoPlanoAnterior, itemsPA_Csw, cronograma, loteCsw
from models.GestaoOPAberto import FilaFases, realizadoFases
import numpy as np
import re

def MetasFase(plano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado = False):
    nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
    novo = ", ".join(nomes_com_aspas)

    if congelado == False:


        sqlMetas = """select "codLote", 
        "Empresa" , "codEngenharia" , "codSeqTamanho" , "codSortimento" , previsao  
        from "PCP".pcp.lote_itens li 
        where  "codLote" in ("""+novo+""")"""

        sqlRoteiro = """
        select * from "PCP".pcp."Eng_Roteiro" er 
        """

        sqlApresentacao = """
        select "nomeFase" , apresentacao  from "PCP".pcp."SeqApresentacao" sa 
        """

        sqlItens = """
        select codigo as "codItem", nome, "unidadeMedida" , "codItemPai" , "codSortimento" as "codSortimento" , "codSeqTamanho" as "codSeqTamanho"  from pcp.itens_csw ic 
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        sqlMetas = pd.read_sql(sqlMetas,conn)
        sqlRoteiro = pd.read_sql(sqlRoteiro,conn)
        sqlApresentacao = pd.read_sql(sqlApresentacao,conn)

        sqlItens = pd.read_sql(sqlItens,conn)
        # Verificar quais codItemPai começam com '1' ou '2'
        mask = sqlItens['codItemPai'].str.startswith(('1', '2'))
        # Aplicar as transformações usando a máscara
        sqlItens['codEngenharia'] = np.where(mask, '0' + sqlItens['codItemPai'] + '-0', sqlItens['codItemPai'] + '-0')

        sqlMetas = pd.merge(sqlMetas,sqlItens,on=["codEngenharia" , "codSeqTamanho" , "codSortimento"],how='left')
        sqlMetas['codItem'].fillna('-',inplace=True)

        saldo = SaldoPlanoAnterior.SaldosAnterior(plano)
        sqlMetas = pd.merge(sqlMetas,saldo,on='codItem',how='left')

        estoque, cargas = itemsPA_Csw.EstoquePartes()
        sqlMetas = pd.merge(sqlMetas,estoque,on='codItem',how='left')

        #cargas = itemsPA_Csw.CargaFases()
        sqlMetas = pd.merge(sqlMetas,cargas,on='codItem',how='left')


        sqlMetas['saldo'].fillna(0,inplace=True)
        sqlMetas['estoqueAtual'].fillna(0,inplace=True)
        sqlMetas['carga'].fillna(0,inplace=True)

        sqlMetas['estoque-saldoAnt'] = sqlMetas['estoqueAtual'] - sqlMetas['saldo']
        sqlMetas['FaltaProgramar1'] = sqlMetas['previsao']-(sqlMetas['estoque-saldoAnt'] + sqlMetas['carga'])
        try:
            sqlMetas['FaltaProgramar'] = sqlMetas.apply(lambda l: l['FaltaProgramar1']if l['FaltaProgramar1'] >0 else 0 ,axis=1 )
        except:
            print('verificar')
        sqlMetas.to_csv('./dados/analise.csv')

        Meta = sqlMetas.groupby(["codEngenharia" , "codSeqTamanho" , "codSortimento"]).agg({"previsao":"sum","FaltaProgramar":"sum"}).reset_index()
        filtro = Meta[Meta['codEngenharia'].str.startswith('0')]
        totalPc = filtro['previsao'].sum()
        totalFaltaProgramar = filtro['FaltaProgramar'].sum()
        novo2 = novo.replace('"',"-")
        Totais = pd.DataFrame([{'0-Previcao Pçs':f'{totalPc} pcs','01-Falta Programar': f'{totalFaltaProgramar} pçs'}])
        Totais.to_csv(f'./dados/Totais{novo2}.csv')



        # Carregando o Saldo COLECAO ANTERIOR

        Meta = pd.merge(Meta,sqlRoteiro,on='codEngenharia',how='left')
        # Converter as colunas para arrays do NumPy
        codFase_array = Meta['codFase'].values
        codEngenharia_array = Meta['codEngenharia'].values

        # Filtrar as linhas onde 'codFase' é 401
        fase_401 = codFase_array == 401
        fase_426 = codFase_array == 426

        # Filtrar as linhas onde 'codEngenharia' não começa com '0'
        nao_comeca_com_0 = np.vectorize(lambda x: not x.startswith('0'))(codEngenharia_array)
        nao_comeca_com_0_426 = np.vectorize(lambda x: not x.startswith('6'))(codEngenharia_array)

        # Combinar as duas condições para filtrar as linhas
        filtro_comb = fase_401 & nao_comeca_com_0
        filtro_comb2 = fase_426 & nao_comeca_com_0_426

        # Aplicar o filtro invertido
        Meta1 = Meta[~filtro_comb]
        Meta = Meta1[~filtro_comb2]


        Meta.to_csv('./dados/analiseFaltaProgrFases.csv')


        Meta = Meta.groupby(["codFase" , "nomeFase"]).agg({"previsao":"sum","FaltaProgramar":"sum"}).reset_index()
        Meta = pd.merge(Meta,sqlApresentacao,on='nomeFase',how='left')
        Meta['apresentacao'] = Meta.apply(lambda x: 0 if x['codFase'] == 401 else x['apresentacao'] , axis=1)

        Meta = Meta.sort_values(by=['apresentacao'], ascending=True)  # escolher como deseja classificar

        cronogramaS =cronograma.CronogramaFases(plano)
        Meta = pd.merge(Meta,cronogramaS,on='codFase',how='left')

        colecoes = TratamentoInformacaoColecao(arrayCodLoteCsw)

        filaFase = FilaFases.ApresentacaoFila(colecoes)
        filaFase = filaFase.loc[:,
                      ['codFase', 'Carga Atual', 'Fila']]

        Meta = pd.merge(Meta,filaFase,on='codFase',how='left')
        Meta['Carga Atual'].fillna(0,inplace=True)
        Meta['Fila'].fillna(0,inplace=True)
        Meta['Falta Produzir'] = Meta['Carga Atual'] + Meta['Fila'] + Meta['FaltaProgramar']
        Meta['dias'].fillna(1,inplace=True)
        Meta['Meta Dia'] = Meta['Falta Produzir'] /Meta['dias']
        Meta['Meta Dia'] = Meta['Meta Dia'] .round(0)

        # Ponto de Congelamento do lote:
        Meta.to_csv(f'./dados/analiseLote{novo2}.csv')

        realizado = realizadoFases.RealizadoMediaMovel(dataMovFaseIni, dataMovFaseFim)
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
        novo2 = novo.replace('"',"-")
        Meta = pd.read_csv(f'./dados/analiseLote{novo2}.csv')
        Totais = pd.read_csv(f'./dados/Totais{novo2}.csv')
        totalPc = Totais['0-Previcao Pçs'][0]
        totalFaltaProgramar = Totais['01-Falta Programar'][0]

        realizado = realizadoFases.RealizadoMediaMovel(dataMovFaseIni, dataMovFaseFim)
        realizado['codFase'] = realizado['codFase'].astype(int)
        Meta = pd.merge(Meta,realizado,on='codFase',how='left')

        Meta['Realizado'].fillna(0,inplace=True)
        Meta.fillna('-',inplace=True)
        Meta = Meta[Meta['apresentacao']!='-']

        dados = {
        '0-Previcao Pçs': f'{totalPc} pcs',
        '01-Falta Programar':f'{totalFaltaProgramar} pçs',
        '1-Detalhamento': Meta.to_dict(orient='records')}

        return pd.DataFrame([dados])

def TratamentoInformacaoColecao(ArraycodLote):

    colecoes = []

    for codLote in ArraycodLote:
        descricaoLote = loteCsw.ConsultarLoteEspecificoCsw('1',codLote)


        if 'INVERNO' in descricaoLote:
            nome = 'INVERNO'+' '+extrair_ano(descricaoLote)
            colecoes.append(nome)
        elif 'PRI' in descricaoLote:
            nome = 'VERAO'+' '+extrair_ano(descricaoLote)
            colecoes.append(nome)
        elif 'ALT' in descricaoLote:
            nome = 'ALTO VERAO'+' '+extrair_ano(descricaoLote)
            colecoes.append(nome)

        elif 'VER' in descricaoLote:
            nome = 'VERAO'+' '+extrair_ano(descricaoLote)
            colecoes.append(nome)
        else:
            nome = 'ENCOMENDAS'+' '+extrair_ano(descricaoLote)
            colecoes.append(nome)

    return colecoes
def extrair_ano(descricaoLote):
    match = re.search(r'\b2\d{3}\b', descricaoLote)
    if match:
        return match.group(0)
    else:
        return None