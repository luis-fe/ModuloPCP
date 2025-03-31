import pandas as pd
from connection import ConexaoBanco

class Produto_Csw():
    '''Classe que interagem via sql com o cadastro de produtos do csw '''


    def __init__(self, codEmpresa = '1'):
        '''construtor da classe '''

        self.codEmpresa = str(codEmpresa) # codEmpresa

    def conversaoSKUparaSKUPartes(self):
        '''Metodo que atribui o codigo sku da Parte ao sku do Produto Acabado
            return:
            DataFrame [{
            redParte, # o codigo sku da Parte Semiacabada
            codItem #o codigo sku do produto pai
            }]
        '''

        sql = """
        SELECT 
            cv.CodComponente as redParte, 
            cv.codProduto, 
            cv2.codSortimento, 
            cv2.seqTamanho as codSeqTamanho, 
            cv2.quantidade
        FROM 
            tcp.ComponentesVariaveis cv
        inner join 
            tcp.CompVarSorGraTam cv2 
            on cv2.codEmpresa = cv.codEmpresa 
            and cv2.codProduto = cv.codProduto 
            and cv.codSequencia = cv2.sequencia 
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

        return relacaoPartes

    def estoqueNaturezaPA(self):
        '''Método que consulta o estoque de PA na natureza 5'''
        sql = """
        SELECT 
            d.codItem , 
            d.estoqueAtual  
        FROM 
            est.DadosEstoque d
        WHERE 
            d.codNatureza = 5 
            and d.codEmpresa = """+self.codEmpresa+"""
            and d.estoqueAtual > 0
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                estoque = pd.DataFrame(rows, columns=colunas)

        return estoque


