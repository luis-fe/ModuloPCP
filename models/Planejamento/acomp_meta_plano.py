import pandas as pd
from connection import ConexaoPostgreWms,ConexaoBanco
from models.Planejamento import SaldoPlanoAnterior, itemsPA_Csw, cronograma

def MetasFase(plano, arrayCodLoteCsw):
    nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
    novo = ", ".join(nomes_com_aspas)

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
    sqlItens['codEngenharia'] = sqlItens.apply(
        lambda r: ('0' + r['codItemPai'] + '-0') if r['codItemPai'].startswith('1') or r['codItemPai'].startswith('2')  else (r['codItemPai'] + '-0'),
        axis=1
    )
    sqlMetas = pd.merge(sqlMetas,sqlItens,on=["codEngenharia" , "codSeqTamanho" , "codSortimento"],how='left')
    sqlMetas['codItem'].fillna('-',inplace=True)

    saldo = SaldoPlanoAnterior.SaldosAnterior(plano)
    sqlMetas = pd.merge(sqlMetas,saldo,on='codItem',how='left')

    estoque = itemsPA_Csw.EstoqueNaturezaPA()
    sqlMetas = pd.merge(sqlMetas,estoque,on='codItem',how='left')

    cargas = itemsPA_Csw.CargaFases()
    sqlMetas = pd.merge(sqlMetas,cargas,on='codItem',how='left')


    sqlMetas['saldo'].fillna(0,inplace=True)
    sqlMetas['estoqueAtual'].fillna(0,inplace=True)
    sqlMetas['carga'].fillna(0,inplace=True)

    sqlMetas['estoque-saldoAnt'] = sqlMetas['estoqueAtual'] - sqlMetas['saldo']
    sqlMetas['FaltaProgramar'] = sqlMetas['previsao']-(sqlMetas['estoque-saldoAnt'] + sqlMetas['carga'])
    sqlMetas['FaltaProgramar'] = sqlMetas.apply(lambda l: l['FaltaProgramar'] if l['FaltaProgramar'] >0 else 0 ,axis=1 )

    sqlMetas.to_csv('./dados/analise.csv')

    Meta = sqlMetas.groupby(["codEngenharia" , "codSeqTamanho" , "codSortimento"]).agg({"previsao":"sum","FaltaProgramar":"sum"}).reset_index()
    filtro = Meta[Meta['codEngenharia'].str.startswith('0')]
    totalPc = filtro['previsao'].sum()
    totalFaltaProgramar = filtro['FaltaProgramar'].sum()

    # Carregando o Saldo COLECAO ANTERIOR

    Meta = pd.merge(Meta,sqlRoteiro,on='codEngenharia',how='left')



    Meta = Meta.groupby(["codFase" , "nomeFase"]).agg({"previsao":"sum","FaltaProgramar":"sum"}).reset_index()
    Meta = pd.merge(Meta,sqlApresentacao,on='nomeFase',how='left')
    Meta = Meta.sort_values(by=['apresentacao'], ascending=True)  # escolher como deseja classificar

    cronogramaS =cronograma.CronogramaFases(plano)
    Meta = pd.merge(Meta,cronogramaS,on='codFase',how='left')
    Meta.fillna('-',inplace=True)


    dados = {
        '0-Previcao Pçs': f'{totalPc} pcs',
        '01-Falta Programar':f'{totalFaltaProgramar} pçs',
        '1-Detalhamento': Meta.to_dict(orient='records')}

    return pd.DataFrame([dados])

