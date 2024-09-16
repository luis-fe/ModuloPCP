import pandas as pd
from connection import ConexaoBanco


class EstoqueSKU():
    '''Classe para obter os itens em estoque a nivel de sku'''

    def __init__(self, naturezas = None):
        self.naturezas = naturezas

    def consultaEstoqueConsolidadoPorReduzido_nat5(self):
        consultasqlCsw = """
        select 
            dt.reduzido as codProduto, 
            SUM(dt.estoqueAtual) as estoqueAtual, 
            sum(estReservPedido) as estReservPedido 
        from
            (select codItem as reduzido, estoqueAtual,estReservPedido  from est.DadosEstoque where codEmpresa = 1 and codNatureza = 5 and estoqueAtual > 0)dt
        group by dt.reduzido
         """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta