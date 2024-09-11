import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms

class Produto():
    def __init__(self, codSku = None):
        self.codSku = codSku

    def conversaoSKUparaSKUPartes(self):
        '''Metodo que atribui o codigo sku da Parte ao sku do Produto Acabado
            return:
            DataFrame [{
            redParte, # o codigo sku da Parte Semiacabada
            codProduto #o codigo sku do produto pai
            }]
        '''

        sql = """
        SELECT cv.CodComponente as redParte, cv.codProduto, cv2.codSortimento, cv2.seqTamanho as codSeqTamanho, cv2.quantidade
        FROM tcp.ComponentesVariaveis cv
        inner join tcp.CompVarSorGraTam cv2 on cv2.codEmpresa = cv.codEmpresa and cv2.codProduto = cv.codProduto and cv.codSequencia = cv2.sequencia 
        WHERE cv.codEmpresa = 1 and cv.codClassifComponente in (10,12) and cv.codProduto like '%-0'
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                relacaoPartes = pd.DataFrame(rows, columns=colunas)

        relacaoPartes['codSeqTamanho'] = relacaoPartes['codSeqTamanho'].astype(str)
        relacaoPartes['codSortimento'] = relacaoPartes['codSortimento'].astype(str)


        sl2Itens2 = """
            select codigo as "codProduto", "codSortimento"::varchar, "codSeqTamanho"::varchar, '0'||"codItemPai"||'-0' as "codProduto"  from "PCP".pcp.itens_csw ic 
            where ic."codItemPai" like '1%'
            """

        conn = ConexaoPostgreWms.conexaoEngine()
        itens = pd.read_sql(sl2Itens2, conn)

        relacaoPartes = pd.merge(relacaoPartes, itens, on=['codProduto', 'codSortimento', 'codSeqTamanho'])

        return relacaoPartes

