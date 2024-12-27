from connection import ConexaoPostgreWms
import pandas as pd
from models import PlanoClass

class Meta ():
    '''Classe utilizada para definicao das METAS do PCP'''


    def __init__(self, codPlano = None, marca = None, metaFinanceira = None , metaPecas = None, nomeCategoria = None):
        '''Contrutor da clasee'''

        self.codPlano = codPlano
        self.marca = marca
        self.metaFinanceira = str(metaFinanceira)
        self.metaPecas = str(metaPecas)
        self.nomeCategoria = nomeCategoria

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

    def inserirMetaCategoriaPlano(self):
        '''Metodo para inserir a meta de categoria '''

        sql = """
        insert into pcp."Meta_Categoria_Plano" 
        ("codPlano", "nomeCategoria" , "marca" , "metaPc" , "metaFinanceira")
        values
        ( %s, %s, %s , %s , %s)
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(sql, (
                self.codPlano, self.nomeCategoria, self.marca, self.metaPecas, self.metaFinanceira))
                conn.commit()


    def updateMetaCategoriaPlano(self):
        '''Metodo que atualiza as metas de categoria no plano '''

        update = """
        update 
            pcp."Meta_Categoria_Plano" 
        set
            "metaPc" = %s , 
            "metaFinanceira" = %s
        where 
            "codPlano" = %s 
            and "marca" = %s
            and "nomeCategoria" = %s
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(update, (
                self.metaPecas, self.metaFinanceira, self.codPlano, self.marca, self.nomeCategoria))
                conn.commit()


    def consultaEspecificaMetaPorCategoriaPlano(self):
        '''metodo de consulta de meta, marca e categoria especifica no plano'''

        sql = """
        select * from pcp."Meta_Categoria_Plano"
        where 
            "codPlano" = %s 
            and "marca" = %s
            and "nomeCategoria" = %s
        """
        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql, conn,params=(self.codPlano, self.marca, self.nomeCategoria))

        return consulta

    def atualizaOuInserirMetaCategoria(self):
        '''Metodo que inseri a meta por categoria '''

        verifica = self.consultaEspecificaMetaPorCategoriaPlano()

        if verifica.empty:
            self.inserirMetaCategoriaPlano()
        else:
            self.updateMetaCategoriaPlano()

        return pd.DataFrame([{'status':True, 'mensagem':'meta da categoria/marca inserido com sucesso !'}])



    def consultarMetaCategoriaPlano(self):
        '''metodo que realiza a consulta da meta por categoria do plano e da Marca '''

        consulta1 = self.consultaMetaGeralPlanoMarca()

        if consulta1.empty:
            totalPecas = 0
            totalReais = 0
        else:
            totalPecas = consulta1['metaPecas'][0].str.replace('.','')
            totalPecas = int(totalPecas)
            totalReais = consulta1['metaFinanceira'][0]
            totalReais = float(totalReais)
        sql1 = """
        select
			m."nomeCategoria" ,
			m."metaPc" ,
			m."metaFinanceira" 
		from
			"PCP".pcp."Meta_Categoria_Plano" m
		where 
			"codPlano" = %s 
			and "marca" = %s
        """

        sql2 ="""
        select
			distinct "nomeCategoria"
		from
			"PCP".pcp."Categorias" c
        """

        conn = ConexaoPostgreWms.conexaoEngine()

        consulta1 = pd.read_sql(sql1,conn,params=(self.codPlano, self.marca))
        consulta1['metaPc'] = consulta1['metaPc'].apply(self.formatar_padraoInteiro)
        consulta1['metaFinanceira'] = consulta1['metaFinanceira'].apply(self.formatar_financeiro)

        consulta2 = pd.read_sql(sql2,conn)

        consulta = pd.merge(consulta2, consulta1, on=['nomeCategoria'],how='left')
        consulta.fillna('-', inplace=True)



        data = {
                '1- Plano - Marca:': f'({self.codPlano})-{self.marca}',
                '2- Total Pecas':f'{totalPecas:,.0f}'.replace(",", "X").replace("X", "."),
                '3 - Total R$':f'R$ {totalReais:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),
                '4- DetalhamentoCategoria': consulta.to_dict(orient='records')
            }
        return pd.DataFrame([data])


    def formatar_financeiro(self,valor):
        try:
            return f'R$ {valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível

    def formatar_padraoInteiro(self,valor):
        try:
            return f'{valor:,.0f}'.replace(",", "X").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível

