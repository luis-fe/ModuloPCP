'''Arquivo utilizado para  importar para o banco as ops em producao ao nivel de op-sku '''

import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
import numpy as np

def PesquisandoReduzido():
    conn = ConexaoPostgreWms.conexaoEngine()

    consulta = """select "codItemPai" as "codProduto"  ,"codSortimento" as "codSortimento" , "codSeqTamanho" as "seqTamanho", "codSKU" as codreduzido from "pcp"."SKU" """

    consulta = pd.read_sql(consulta,conn)

    consulta['codProduto'] = consulta['codProduto'] + "-0"
    consulta['codProduto'] = consulta['codProduto'].apply(lambda x: '0'+ x if x.startswith(('1', '2')) else x)
    consulta['codSortimento'] = consulta['codSortimento'] .astype(str)
    consulta['seqTamanho'] = consulta['seqTamanho'] .astype(str)

    return consulta



def BuscandoOPCSW(empresa):

    sqlCswOpsnivelSku = """
        SELECT op.codfaseatual ,ot.codProduto ,ot.numeroop as numeroop , ot.codSortimento , seqTamanho, 
        case WHEN ot.qtdePecas1Qualidade is null then ot.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs 
        FROM tco.OrdemProdTamanhos ot
        inner join tco.ordemprod op on op.codempresa = ot.codempresa and op.numeroop = ot.numeroop
        having ot.codEmpresa = """ + empresa + """ and ot.numeroOP IN """ + ' (select o.numeroOP  from tco.ordemprod o where o.situacao = 3 and o.codempresa = ' + empresa + ')'

    sqlCsw_em_aberto2 = ' select o.numeroOP as numeroop,  o.codTipoOP, codSeqRoteiroAtual as seqAtual  from tco.ordemprod o where o.situacao = 3 and o.codempresa = ' + empresa
    with ConexaoBanco.ConexaoInternoMPL() as conn:  ##Abrindo Conexao Com o CSW
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sqlCswOpsnivelSku)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            get = pd.DataFrame(rows, columns=colunas)
            del rows

            cursor_csw.execute(sqlCsw_em_aberto2)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            em_aberto2 = pd.DataFrame(rows, columns=colunas)
            del rows

            get = pd.merge(get, em_aberto2, on='numeroop')
            sku = PesquisandoReduzido()
            get['codProduto'] = get['codProduto'].astype(str)
            get['codSortimento'] = get['codSortimento'].astype(str)
            get['seqTamanho'] = get['seqTamanho'].astype(str)
            get = pd.merge(get, sku, on=["codProduto", "codSortimento", "seqTamanho"], how='left')
            # Atribui valores iniciais à coluna 'id' com base na coluna 'codTipoOP'
            get['id'] = np.where(get['codTipoOP'].isin([1, 4]), 9000, 2000)
            # Atualiza os valores da coluna 'id' com base na coluna 'codfaseatual'
            get['id'] = np.where(get['codFaseAtual'].isin(['1', '401']), 1000, get['id'])

            get['id'] = get['id'] + get['seqAtual'].astype(int)

            # contagem de duplicaçoes : reduzido + codTipoOP + codFaseAtual
            get['pesquisa'] = get.groupby(['codreduzido','codTipoOP','codFaseAtual']).count() + 1

            get2 = get[get['pesquisa']>1]
            print(get2)

            get['id'] = get['id'].astype(str) + '||' + get['codreduzido'].astype(str)
            get = get.sort_values(by=['codreduzido', 'id'], ascending=False)  # escolher como deseja classificar

            get['ocorrencia_sku'] = get.groupby('codreduzido').cumcount() + 1
            get['id'] = get['id'].astype(str) + '||' + get['ocorrencia_sku'].astype(str)
            get['qtdAcumulada'] = get.groupby('codreduzido')['total_pcs'].cumsum()
            return get


def IncrementadoDadosPostgre(empresa):

        dados = BuscandoOPCSW(empresa)
        ConexaoPostgreWms.Funcao_InserirOFF(dados,dados['numeroop'].size,'ordemprod','replace')
        return pd.DataFrame([{'status':True,'Mensagem':'Dados carregados com sucesso !'}])

