import pandas as pd
from connection import ConexaoPostgreWms
from models import Produto_CSW

class Produtos():
    '''Classe utilizada para interagir  com os Produtos'''


    def __init__(self, codEmpresa = '1'):
        '''construtor '''

        self.codEmpresa = codEmpresa # Atributo codigo da empresa

        self.produtoCsw = Produto_CSW.Produto_Csw(self.codEmpresa) # Instanciando o objeto para classe ProdutoCSW


    def roteiro_Engenharias(self):
        '''Metodo que busca os roteiros da Engenharia '''

        conn = ConexaoPostgreWms.conexaoEngine()
        # Sql que obtem os roteiros das engenharias
        sqlRoteiro = """
            select 
                * 
            from 
                "PCP".pcp."Eng_Roteiro" er 
        """

        consulta = pd.read_sql(sqlRoteiro, conn)

        return consulta


    def itens_tam_cor(self):
        '''Metodo que busca os itens a nivel de tamanho e cor'''

        conn = ConexaoPostgreWms.conexaoEngine()


        consulta = """
        select 
            codigo as "codItem", nome, "unidadeMedida" , "codItemPai" , "codSortimento" as "codSortimento" , "codSeqTamanho" as "codSeqTamanho" , categoria 
        from pcp.itens_csw ic 
        """

        consulta = pd.read_sql(consulta, conn) #ESTIMATIVA DE TEMPO DE PROCESSAMENTO DA QUERY: 7 - 15 Segundos '''

        return consulta

    def relacao_Partes_Pai(self):
        '''Metodo que relaciona a parte pai x parte filha '''

        relacaoPartes = self.produtoCsw.conversaoSKUparaSKUPartes()
        sl2Itens2 = """
            select 
                codigo as "codItem", 
                "codSortimento"::varchar, 
                "codSeqTamanho"::varchar, 
                '0'||"codItemPai"||'-0' as "codProduto"  
            from 
                "PCP".pcp.itens_csw ic 
            where 
                ic."codItemPai" like '1%'
            """

        conn = ConexaoPostgreWms.conexaoEngine()
        itens = pd.read_sql(sl2Itens2, conn)

        self.relacaoPartes = pd.merge(relacaoPartes, itens, on=['codProduto', 'codSortimento', 'codSeqTamanho'])


    def estoqueProdutosPA_addPartes(self):
        '''Metodo que consulta o estoque dos Produtos e adiciona tambem o estoque da relacao Pai x Partes '''

        estoques = self.produtoCsw.estoqueNaturezaPA()
        estoque_relacaoPartes = pd.merge(self.relacaoPartes , estoques, on='codItem')

        estoque_relacaoPartes['estoqueAtual'] = estoque_relacaoPartes['quantidade'] * estoque_relacaoPartes['estoqueAtual']
        estoque_relacaoPartes.drop(['codItem', 'codProduto', 'codSortimento', 'codSeqTamanho'], axis=1, inplace=True)
        estoque_relacaoPartes.rename(columns={'redParte': 'codItem'}, inplace=True)


        estoqueProdutos = pd.concat([estoques,estoque_relacaoPartes])
        estoqueProdutos = estoqueProdutos.groupby('codItem').agg(
            {'quantidade': 'first', 'estoqueAtual': 'first'}).reset_index()

        return estoqueProdutos

