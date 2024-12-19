from connection import ConexaoPostgreWms
import pandas as pd
from models import PlanoClass

class Meta ():
    '''Classe utilizada para definicao das METAS do PCP'''


    def __init__(self, codPlano = None, marca = None, metaFinanceira = None , metaPecas = None):
        '''Contrutor da clasee'''

        self.codPlano = codPlano
        self.marca = marca
        self.metaFinanceira = str(metaFinanceira)
        self.metaPecas = str(metaPecas)

    def consultaMetaGeral(self):
        '''Método utilizado para consultar a Meta Geral.'''

        sql = """
        SELECT 
            "codPlano",
            p."descricaoPlano",
            "marca",
            "metaFinanceira",
            "metaPecas"
        FROM
            "pcp"."Metas" m
        INNER JOIN
            "pcp"."Plano" p ON p.codigo = m."codPlano" 
        WHERE
            "codPlano" = %s
        """

        # Obtem a conexão com o banco
        conn = ConexaoPostgreWms.conexaoEngine()

        # Realiza a consulta no banco de dados
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        # Função para tratar e formatar a string "R$xxxxxxx"
        def formatar_meta_financeira(valor):
            try:
                valor_limpo = float(valor.replace("R$", "").replace(",", "").strip())
                return f'R$ {valor_limpo:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
            except ValueError:
                return valor  # Retorna o valor original caso não seja convertível


        def formatar_meta_pecas(valor):
            try:
                valor_limpo = int(valor)
                return f'{valor_limpo:,.0f}'.replace(",", "X").replace("X", ".")
            except ValueError:
                return valor  # Retorna o valor original caso não seja convertível

        def formatar_meta_financeira_int(valor):
                # Remove o prefixo "R$", pontos e vírgulas, e converte para float
                valor_limpo = int(valor.replace("R$", "").replace(".", ""))
                return valor_limpo

        def formatar_meta_financeira_float(valor):
                # Remove o prefixo "R$", pontos e vírgulas, e converte para float
                valor_limpo = float(valor.replace("R$", "").replace(".", "").replace(",", ".").strip())
                return valor_limpo



        # Aplica o tratamento à coluna 'metaFinanceira' (formatação monetária)
        consulta['metaFinanceira'] = consulta['metaFinanceira'].apply(formatar_meta_financeira)
        consulta['metaPecas'] = consulta['metaPecas'].apply(formatar_meta_pecas)


        totalFinanceiro = consulta['metaFinanceira'].apply(formatar_meta_financeira_float).sum()
        totalPecas = consulta['metaPecas'].apply(formatar_meta_financeira_int).sum()

        # Cria a linha de total
        total = pd.DataFrame([{
            'codPlano': self.codPlano,
            'descricaoPlano': consulta['descricaoPlano'].iloc[0],
            'marca': 'TOTAL',
            'metaFinanceira': f'R$ {totalFinanceiro:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),  # Soma os valores numéricos
            'metaPecas': f'{totalPecas:,.0f}'.replace(",", "X").replace("X", ".")
        }])

        # Concatena o total ao DataFrame original
        consulta = pd.concat([consulta, total], ignore_index=True)

        return consulta

    def consultaMetaGeralPlanoMarca(self):
        ''''Metodo para consultar as metas de um plano por Plano e Marca '''

        sql = """
        select 
            "codPlano",
            "marca",
            "metaFinanceira",
            "metaPecas"
        from
            "pcp"."Metas"
        where
            "codPlano" = %s and "marca" = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano, self.marca))

        return consulta




    def cadastrarMetaGeral(self):
        '''metodo criado para cadastrar as metas gerais '''

        insert = """
        insert into "pcp"."Metas"
        (   "codPlano",
            "marca",
            "metaFinanceira",
            "metaPecas")
        values (%s, %s, %s, %s)
        """

        self.metaFinanceira = 'R$' + self.metaFinanceira

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(insert,(self.codPlano, self.marca, self.metaFinanceira, self.metaPecas))


    def updateMetaGeral(self):


        update = """
        update "pcp"."Metas"
        set
            "metaFinanceira" = %s,
            "metaPecas" = %s
        where
        "marca" = %s and "codPlano" = %s
        
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(update,(self.metaFinanceira, self.metaPecas, self.marca, self.codPlano))



    def inserirOuAtualizarMetasGeraisPlano(self):
        '''Metodo utilizado para inserir ou atualizar as metas gerais por Plano e Marca'''

        verifica = self.consultaMetaGeralPlanoMarca()

        if verifica.empty:
            self.cadastrarMetaGeral()
            self.inserirMetaSemanalPlanoMarca()
        else:
            self.updateMetaGeral()

        return pd.DataFrame([{'status':True, "mensagem":'Metas inseridas com sucesso'}])

    def inserirMetaSemanalPlanoMarca(self):
        '''Metodo que verica quantas semanas de vendas possue o plano, e prepara os dados'''

        insert = """
        insert into "pcp"."MetasSemanal" ("codPlano", "marca", semana, distribuicao ,"metaFinanceira", "metaPeca" )
        values ( %s, %s, %s , %s ,%s, %s )
        """


        numeroSemanas = PlanoClass.Plano(self.codPlano).obterNumeroSemanasVendas()

        self.distribuicao = '0'
        self.metaFinanceira = '0'
        self.metaPecas = '0'

        if numeroSemanas > 0:
            for i in range(numeroSemanas):
                self.semana = i +1

                with ConexaoPostgreWms.conexaoInsercao() as conn:
                    with conn.cursor() as curr:
                        curr.execute(insert,(self.codPlano, self.marca, self.semana, self.distribuicao, self.metaFinanceira, self.metaPecas))
                        conn.commit()

    def consultaMetaSemana(self):
        '''Metodo que consulta as metas a nivel semanal '''

        sql = """
        select 
            "codPlano", "marca", semana, distribuicao ,"metaFinanceira", "metaPeca" 
        from 
            pcp."MetasSemanal"
        where 
            "codPlano" = %s
        order by 
            semana asc
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        return consulta

    def atualizarMetasSemanais(self):
        '''Metodo utilizado para atualizar a meta semanal'''

        update = """
        update 
            pcp."MetasSemanal"
        set
             distribuicao = %s ,
             "metaFinanceira" = %s ,
             "metaPeca" = %s
        where
             "codPlano" = %s
             and "semana" = %s
             and "marca" = %s
        """









