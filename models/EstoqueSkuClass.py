import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms


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

    def simulandoEstoqueGarantia(self):

        sql = """
select
	o.codreduzido as "codProduto",
sum("total_pcs") as "estoqueAtual",0 as estReservPedido
from
	"pcp".ordemprod o
where
	"qtdAcumulada" > 0 and ("codFaseAtual" in ('441' ,'466','437') or "numeroop" in (
	'138771-001',
'138875-001',
'138958-001',
'139470-001',
'140578-001',
'140588-001',
'140612-001',
'140623-001',
'140783-001',
'140968-001',
'141031-001'
	))
group by codreduzido
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        estoqueCsw = self.consultaEstoqueConsolidadoPorReduzido_nat5()

        concatenar = pd.concat([estoqueCsw, consulta], ignore_index=True)

        group = concatenar.groupby(["codProduto"]).agg({"estoqueAtual":"sum","estReservPedido":"sum"}).reset_index()

        return group

