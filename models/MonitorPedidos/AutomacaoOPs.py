'''Arquivo utilizado para  importar para o banco as ops em producao ao nivel de op-sku '''

import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
def BuscandoOPCSW(empresa):
    with ConexaoBanco.Conexao2() as conn:  ##Abrindo Conexao Com o CSW
        sqsCswOPem_aberto = ' (select o.numeroOP  from tco.ordemprod o where o.situacao = 3 and o.codempresa = ' + empresa + ')'

        sqlCswOpsnivelSku = """
        SELECT ot.codProduto ,ot.numeroop as numeroop , codSortimento , seqTamanho, 
        case WHEN ot.qtdePecas1Qualidade is null then ot.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs '
        FROM tco.OrdemProdTamanhos ot
        having ot.codEmpresa = " + empresa + " and ot.numeroOP IN """ + sqsCswOPem_aberto

        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sqlCswOpsnivelSku)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            get = pd.DataFrame(rows, columns=colunas)
            del rows