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


        consultar2 = """
        select
            "Ncarrinho",
            numeroop,
            cor ,
            tamanho,
            count(codbarrastag) as "Pecas"
        from
            "off".reposicao_qualidade rq
        where
            "Ncarrinho" = %s and codempresa = %s
            and (rq."statusNCarrinho" <> 'liberado' or rq."statusNCarrinho" is null)
        group by
            "Ncarrinho",
            numeroop,
            cor ,
            tamanho
        """

        conn = ConexaoPostgreWms.conexaoEngineWms()

        consulta = pd.read_sql(consultar2, conn, params=(self.Ncarrinho, self.empresa))

        return consulta

    def atribuirOPRevisor(self):
        '''Metodo utilizado para atribuir o revisor de cada OP'''

        insert = """
        insert into "pcp"."ProdutividadeRevisor" ("empresa","Ncarrinho", "numeroop", "Pecas" , "codRevisor","dataHora")
        values (%s ,%s ,%s, %s, %s, %s)
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


    def produtividadePeriodo(self, dataInicio, dataFinal):
        '''Metodo que retorna a produtividade dos revisores no Periodo'''

        sql = """
        select
            pr."codRevisor" ,
            r."nomeRevisor",
            sum("Pecas") as "Pçs"
        from
            pcp."ProdutividadeRevisor" pr
        inner join
            pcp."Revisor" r on r."codRevisor" = pr."codRevisor" 
        where 
            pr."dataHora"::date >= %s
            and 
            pr."dataHora"::date <= %s
        group by 
            pr."codRevisor" ,
            r."nomeRevisor"
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(dataInicio, dataFinal))
        consulta = consulta.sort_values(by=['Pçs'], ascending=False)


        totalPc=consulta['Pçs'].sum()

        record = self.recordRevisor()
        revisorRecord = record['nomeRevisor'][0]
        dataRecord = record['dataHora'][0]
        PcsRecord = record['Pçs'][0]

        consulta.loc[consulta['nomeRevisor'] == revisorRecord, 'nomeRevisor'] = '👑' + consulta['nomeRevisor']

        dados = {
            '0-Total Pçs': f'{totalPc} pcs',
            '1-RevisorRecord':f'👑{revisorRecord}',
            '2-PcsRecord': f'{PcsRecord}',
            '3-dataRecord': f'{dataRecord}',
            '4-Detalhamento': consulta.to_dict(orient='records')}

        return pd.DataFrame([dados])

    def recordRevisor(self):
        '''Metodo que retorna o recodr na revisao'''

        sql = """
        select
            pr."codRevisor",
            r."nomeRevisor",
            pr."dataHora"::date,
            sum("Pecas") as "Pçs"
        from
            pcp."ProdutividadeRevisor" pr
        inner join
            pcp."Revisor" r on r."codRevisor" = pr."codRevisor" 
        group by 
            pr."codRevisor" ,
            r."nomeRevisor",
            pr."dataHora"::date
        order by 
            sum("Pecas") desc 
        limit 
            1
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def detalharCarrinhoDesmembramento(self):
        '''Metodo utilizado para detalhar o carrinho para fazer desmembramento'''

        consulta = """
        select
            o.numeroop ,
            o.codreduzido,
            sum(total_pcs) as total_pcs 
        from
            "PCP".pcp.ordemprod o
        group by
            o.numeroop,
            codreduzido
        """

        consulta2 = """
        select
            "codreduzido",
            numeroop,
            cor,
            tamanho,
            count(codbarrastag) as "Pecas"
        from
            "off".reposicao_qualidade rq
        where
            "Ncarrinho" = %s and codempresa = %s
            and (rq."statusNCarrinho" <> 'liberado' or rq."statusNCarrinho" is null)
        group by
            numeroop,
            cor,
            tamanho,
            codreduzido
        """

        # Conexões com o banco
        conn = ConexaoPostgreWms.conexaoEngineWms()
        conn2 = ConexaoPostgreWms.conexaoEngine()

        # Executando consultas SQL
        consulta = pd.read_sql(consulta, conn2)
        consulta2 = pd.read_sql(consulta2, conn, params=(self.Ncarrinho, self.empresa))

        # Fazendo merge entre as consultas
        consulta2 = pd.merge(consulta, consulta2, on=['numeroop', 'codreduzido'], how='left')
        consulta2.fillna('-', inplace=True)

        # Criando a coluna 'PcBipadas/Total' e removendo colunas não desejadas
        consulta2['PcBipadas/Total'] = consulta2['Pecas'].astype(str) + '/' + consulta2['total_pcs'].astype(str)
        consulta2 = consulta2.drop(['total_pcs', 'codreduzido', 'Pecas'], axis=1)

        # Agrupando e criando a coluna 'tamanhos-PcBipadas/Total' com listas de pares tamanho-PcBipadas/Total
        consulta2 = (
            consulta2.groupby(['Ncarrinho', 'numeroop', 'cor'])
            .apply(lambda x: [f"{row['tamanho']}-{row['PcBipadas/Total']}" for _, row in x.iterrows()])
            .reset_index(name="tamanhos-PcBipadas/Total")
        )

        return consulta2








