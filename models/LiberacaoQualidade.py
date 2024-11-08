import pandas as pd
from connection import ConexaoPostgreWms
from datetime import datetime
import pytz
class Liberacao():
    def __init__(self, Ncarrinho, codRevisor, empresa, numeroOP =None, Pecas = 1):

        self.Ncarrinho = str(Ncarrinho)
        self.codRevisor = str(codRevisor)
        self.empresa = str(empresa)
        self.numeroOP = str(numeroOP)
        self.Pecas = int(Pecas)


    def consultarCargaCarrinho(self):
        """Metodo utilizado para consultar as OPs produzidas em um carrinho """

        consultar = """
        select
            "Ncarrinho",
	        numeroop as "numeroOP",
	        count(codbarrastag) as "Pecas"
	    from 
	        "off".reposicao_qualidade rq
        where
	        "Ncarrinho" = %s
	        and codempresa = %s
	        and (rq."statusNCarrinho" <> 'liberado' or rq."statusNCarrinho" is null)
        group by
	        numeroop,
	        "Ncarrinho"
        """

        conn = ConexaoPostgreWms.conexaoEngineWms()

        consulta = pd.read_sql(consultar, conn, params=(self.Ncarrinho, self.empresa))

        return consulta

    def atribuirOPRevisor(self):
        '''Metodo utilizado para atribuir o revisor de cada OP'''

        insert = """
        insert into "pcp"."ProdutividadeRevisor" ("empresa","Ncarrinho", "numeroop", "Pecas" , "codRevisor","dataHora")
        values (%s ,%s ,%s, %s, %s, %s, %s)
        """

        self.dataHora = self.obterHoraAtual()
        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(insert, (self.empresa, self.Ncarrinho, self.numeroOP, self.Pecas, self.codRevisor,self.dataHora))
                conn.commit()

        return pd.DataFrame([{'status': True, 'Mensagem': 'Os Revisores foram  Atribuido com sucesso !!'}])


    def atribuirOPRevisorArray(self, array):
        '''Metodo utilizado para atribuir o revisor de cada OP via array'''

        for item in array:
            self.empresa = item[0]
            self.Ncarrinho = item[1]
            self.numeroOP = item[2]
            self.Pecas = item[3]
            self.codRevisor = item[4]

            self.atribuirOPRevisor()

        return pd.DataFrame([{'status': True, 'Mensagem': 'Os Revisores foram  Atribuido com sucesso !!'}])

    def obterHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y/%m/%d %H:%M:%S')

        return agora







