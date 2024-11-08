import pandas as pd
from connection import ConexaoPostgreWms
class Liberacao():
    def __init__(self, Ncarrinho, codRevisor, empresa):

        self.Ncarrinho = str(Ncarrinho)
        self.codRevisor = str(codRevisor)
        self.empresa = str(empresa)


    def consultarCargaCarrinho(self):
        """Metodo utilizado para consultar as OPs produzidas em um carrinho """

        consultar = """
        select
	        numeroop ,
	        count(codbarrastag) as "Pecas"
	    from 
	        "off".reposicao_qualidade rq
        where
	        "Ncarrinho" = %s
	        and codempresa = %s
	        and (rq."statusNCarrinho" <> 'liberado' or rq."statusNCarrinho" is null)
        group by
	        numeroop
        """

        conn = ConexaoPostgreWms.conexaoEngineWms()

        consulta = pd.read_sql(consultar, conn, params=(self.Ncarrinho, self.empresa))

        return consulta
