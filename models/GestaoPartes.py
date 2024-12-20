import gc
import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms


class GestaoPartes():
    '''Classe utilizada para fazer o gerenciamento das Chamadas partes '''
    def __init__(self, numeroOP = None, codFaseAguardandoPartes = None, codFaseMontagem = None, codFaseAguarPecas = None):
        self.numeroOP = numeroOP
        self.codFaseAguardandoPartes = codFaseAguardandoPartes
        self.codFaseMontagem = codFaseMontagem
        self.codFaseAguarPecas = codFaseAguarPecas
    def validarAguardandoPartesOPMae(self):
        '''Metodo que avalia se as Ops programadas que utilizam partes possue a fase aguardando Partes
            e retorna: a fila de OPs antes do AGUARDANDO PARTES;
        '''

        # 1 Verificar as Ops que possuem cadastro de fase

        # 1.1 Carregar as OPs Programadas
        opProgramada = self.opsProgramadas()

        # 1.2 Carregar Produtos que possuem partes cadastradas em componentes
        cadastro = self.carregarProdutosComPartesCadastradas()

        #1.3 Filtrar as OPs que tem partes cadastradas
        opProgramada = pd.merge(opProgramada,cadastro ,on='codProduto')

        #1.4 Carregar as Ops que possuem a fase de montagem
        opsProgramadaFaseMontagem = self.ordemProducaoProgramadaAntesMontagem()


        #1.5 Filtrar as Ops que estao antes da fase Montagem
        df_merged = pd.merge(opProgramada, opsProgramadaFaseMontagem, on='numeroOP')

        #1.6 converendo os campos em inteiro para fazer a comparacao
        df_merged['codSeqRoteiroAtual'] = df_merged['codSeqRoteiroAtual'].astype(int)
        df_merged['rotMax'] = df_merged['rotMax'].astype(int)

        df_filtrado = df_merged[df_merged['codSeqRoteiroAtual'] <= df_merged['rotMax']]


        #1.7 verificando se a OP possue a fase aguardando Partes
        agPartes = self.ordemProducaoComRoterioAguardandoPartes()
        df_filtrado = pd.merge(df_filtrado, agPartes , on='numeroOP', how='left')
        df_filtrado['possueFaseAgPartes'].fillna('Nao',inplace = True)

        # Selecionar apenas as colunas desejadas
        resultado = df_filtrado[['numeroOP', 'codSeqRoteiroAtual','codProduto','codFaseAtual','possueFaseAgPartes']]


        return resultado



    def carregarProdutosComPartesCadastradas(self):
        '''Metodo que carrega os produtos com as partes cadastras no Erp csw'''

        sql = """
        SELECT
            DISTINCT c.codProduto
        FROM
            tcp.ComponentesVariaveis c
        WHERE
            codClassifComponente in (12, 10)
            and c.codEmpresa = 1
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta

    def opsProgramadas(self):
        '''Metodo que retorna as OPs programas, antes da fase Montagem'''

        sql = """
        SELECT 
            o.numeroOP ,
            o.codProduto, 
            o.codFaseAtual,
            o.codSeqRoteiroAtual
        from
            tco.OrdemProd o
        WHERE 
            o.codempresa = 1
            AND o.situacao = 3
            and o.numeroOP like '%-001'
        """


        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


    def ordemProducaoProgramadaAntesMontagem(self):
        '''Metodo que retorna as OPs programadas que estao antes da fase de montagem'''

        sql = """
        SELECT 
            r.numeroOP,
            r.codSeqRoteiro as rotMax  
        FROM 
            tco.RoteiroOP r
        WHERE 
            r.codEmpresa = 1 and r.numeroOP in (
                SELECT 
                    o.numeroOP 
                from
                    tco.OrdemProd o
                WHERE 
                    o.codempresa = 1
                    AND o.situacao = 3
                    and o.numeroOP like '%-001' 
        ) 
        and ( r.codfase ="""+ str(self.codFaseAguardandoPartes)+""" or r.codfase ="""+ str(self.codFaseAguarPecas)+""")"""


        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta

    def ordemProducaoComRoterioAguardandoPartes(self):
        '''Metodo que retorna as OPs programadas que possuem a fase aguardando partes'''

        sql = """
        SELECT 
            r.numeroOP, codfase as possueFaseAgPartes  
        FROM 
            tco.RoteiroOP r
        WHERE 
            r.codEmpresa = 1 and r.numeroOP in (
                SELECT 
                    o.numeroOP 
                from
                    tco.OrdemProd o
                WHERE 
                    o.codempresa = 1
                    AND o.situacao = 3
                    and o.numeroOP like '%-001' 
        ) 
        and r.codfase in("""+ str(self.codFaseAguardandoPartes)+""","""+str(self.codFaseAguarPecas)+""")"""


        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta

    def detalharOPMaeGrade(self):
        '''Metodo utilizado para detalhar a OP mae a nivel de grade tamanho e cor'''

        sqlPortal = """
        select
            numeroop as "numeroOP",
            "codProduto",
            total_pcs as "qtdOPMae",
            "seqTamanho" ,
            "codSortimento",
            codreduzido as "codItem"  
        from
	        "PCP".pcp.ordemprod o
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sqlPortal,conn)

        # 2: Obtendo as ops do Csw na fase 401-PCP a nivel de tamanho e cor
        opPCP =self.ordemProdPCPTamCor()
        # 2.1: realizando o contatenar entre as 2 consultas de op
        consulta = pd.concat([consulta, opPCP], ignore_index=True)


        validarAguardandoPartesOPMae = self.validarAguardandoPartesOPMae()

        consulta = pd.merge(validarAguardandoPartesOPMae,consulta,on=['numeroOP','codProduto'],how='left')


        #Convertendo Sortimento em CodCor
        conversaoCOr = self.convertendoSortimentoCor()
        conversaoCOr['codSortimento'] = conversaoCOr['codSortimento'].astype(str)
        consulta = pd.merge(consulta,conversaoCOr,on=['codSortimento','codProduto'],how='left')

        #Obtendo o codigo do item relacionado a parte
        conversaoCodigo = self.converterPaiemParte()

        consulta = pd.merge(consulta,conversaoCodigo,on='codItem',how='left')
        consulta.fillna('-',inplace=True)



        return consulta

    def convertendoSortimentoCor(self):
        '''Metodo que converte sortimento em Cor'''

        sql = """
        SELECT
	s.codProduto ,
	s.codSortimento ,
	s.corBase as codCor
FROM
	tcp.SortimentosProduto s
WHERE
	s.codEmpresa = 1
	and s.codProduto 
in (
	SELECT
		op.codproduto
	FROM
		tco.OrdemProd op
	WHERE
		op.codempresa = 1
		and op.situacao = 3)
        """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


    def ordemProdAbertoPosMontagem(self):
        '''Metodo que retorna as OP apos da Montagem'''
        # 1.1 Carregar as OPs Programadas
        opProgramada = self.opsProgramadas()

        # 1.2 Carregar Produtos que possuem partes cadastradas em componentes
        cadastro = self.carregarProdutosComPartesCadastradas()

        # 1.3 Filtrar as OPs que tem partes cadastradas
        opProgramada = pd.merge(opProgramada, cadastro, on='codProduto')

        # 1.4 Carregar as Ops que possuem a fase de montagem
        opsProgramadaFaseMontagem = self.ordemProducaoProgramadaAntesMontagem()

        # 1.5 Filtrar as Ops que estao antes da fase Montagem
        df_merged = pd.merge(opProgramada, opsProgramadaFaseMontagem, on='numeroOP')

        # 1.6 converendo os campos em inteiro para fazer a comparacao
        df_merged['codSeqRoteiroAtual'] = df_merged['codSeqRoteiroAtual'].astype(int)
        df_merged['rotMax'] = df_merged['rotMax'].astype(int)

        df_filtrado = df_merged[df_merged['codSeqRoteiroAtual'] > df_merged['rotMax']]

        # Selecionar apenas as colunas 'OP' e 'fase atual'
        resultado = df_filtrado[['numeroOP', 'codSeqRoteiroAtual','codProduto','codFaseAtual']]

        sqlPortal = """
                select
                    numeroop as "numeroOP",
                    "codProduto",
                    total_pcs as "qtdOPMae",
                    "seqTamanho" ,
                    "codSortimento" 
                from
        	        "PCP".pcp.ordemprod o
                """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sqlPortal, conn)

        consulta = pd.merge(resultado, consulta, on=['numeroOP', 'codProduto'], how='left')

        # Convertendo Sortimento em CodCor
        conversaoCOr = self.convertendoSortimentoCor()
        conversaoCOr['codSortimento'] = conversaoCOr['codSortimento'].astype(str)
        consulta = pd.merge(consulta, conversaoCOr, on=['codSortimento', 'codProduto'], how='left')

        return consulta

    def EstoqueProgramadonatureza20(self):
        '''Metodo que retorna o estoque projetado FUTURO para a natureza 20'''
        sql = """
            select
                o.codreduzido as "codItem" ,
                o."codProduto",
                o."codSortimento" ,
                o."seqTamanho",
                o.numeroop,
                o.total_pcs as "qtdOPMae"
            from
                "PCP".pcp.ordemprod o
            where
                o."codProduto" like '6%' or o."codProduto" like '5%' 
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql, conn)

        # Libera memória manualmente
        gc.collect()
        # Substituir os dois primeiros dígitos por "01"
        conversaoCOr = self.convertendoSortimentoCor()
        conversaoCOr['codSortimento'] = conversaoCOr['codSortimento'].astype(str)
        consulta['codSortimento'] = consulta['codSortimento'].astype(str)

        consulta = pd.merge(consulta, conversaoCOr, on=['codSortimento', 'codProduto'], how='left')
        consulta = consulta.groupby(["codProduto","seqTamanho","codCor","codItem"]).agg({"qtdOPMae":"sum"}).reset_index()

        return consulta

    def estruturaItensPartes(self):
        '''Metodo que retorna a estuturacao dos itens Semiacabado partes'''

        # 1 - Carregando as ordem de producao em aberto das Partes (Fonte PCP : vem da automacao do CSW):
        sql = """
	        select
                ic.codigo as "codItem",
                ic.nome,
                ic."codSortimento"::int ,
                ic."codSeqTamanho"::int,
                ic."codItemPai" ||'-0' as "codProduto",
                '0' as "estoqueAtual"
            from
                pcp.itens_csw ic
            where
                 ic.codigo in (
                select
                o.codreduzido 
            from
                "PCP".pcp.ordemprod o
            where
                o."codProduto" like '6%' or o."codProduto" like '5%' 
                )
        """
        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql, conn)

        # 2: convertendo codSortimento em cor:
        conversaoCOr = self.convertendoSortimentoCor2()
        conversaoCOr['codSortimento'] = conversaoCOr['codSortimento'].astype(str)
        consulta['codSortimento'] = consulta['codSortimento'].astype(str)
        consulta = pd.merge(consulta, conversaoCOr, on=['codSortimento', 'codProduto'], how='left')

        # 3: obetndo do ERP CSW o estoque das Partes
        sql2 = """
        SELECT
            d.codItem,
            d.estoqueAtual,
            i2.codItemPai || '-0' as codProduto,
            i2.codCor,
            i2.codSortimento ,
            i2.codSeqTamanho,
            i.nome
        FROM
            est.DadosEstoque d
        inner join
            Cgi.Item2 i2 on
            i2.Empresa = 1
            and i2.codItem = d.codItem
        inner JOIN 
            cgi.Item i on i.codigo = i2.codItem 
        WHERE
            d.codEmpresa = 1
            and d.codNatureza = 20
            and d.estoqueAtual > 0
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql2)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta2 = pd.DataFrame(rows, columns=colunas)
        del rows
        gc.collect()

        # 4: Realizando o groupBy das informacoes

        consulta['codSortimento'] = consulta['codSortimento'].astype(str)
        consulta2['codSortimento'] = consulta2['codSortimento'].astype(str)
        consulta['codSeqTamanho'] = consulta['codSeqTamanho'].astype(str)
        consulta2['codSeqTamanho'] = consulta2['codSeqTamanho'].astype(str)


        consulta = pd.concat([consulta, consulta2], ignore_index=True)
        consulta['estoqueAtual'] = consulta['estoqueAtual'].astype(str)

        consulta = consulta.groupby(["codItem","codProduto","codCor","codSortimento","codSeqTamanho","nome"]).agg({"estoqueAtual":"sum"}).reset_index()

        # 5: obtendo as informacoes do Tamanho
        consulta3 = self.obterDescricaoTamCsw()
        consulta = pd.merge(consulta, consulta3, on='codSeqTamanho', how='left')
        consulta['estoqueAtual'].fillna(0,inplace=True)

        return consulta

    def obterDescricaoTamCsw(self):
        sql3 = """
        	SELECT
                t.sequencia as codSeqTamanho,
                t.descricao as tam
            FROM
                tcp.Tamanhos t
            WHERE
                t.codEmpresa = 1
        """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql3)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta3 = pd.DataFrame(rows, columns=colunas)
        # Libera memória manualmente
        del rows
        gc.collect()
        consulta3['codSeqTamanho'] = consulta3['codSeqTamanho'].astype(str)
        return consulta3

    def convertendoSortimentoCor2(self):
            '''Metodo que converte sortimento em Cor'''

            sql = """
        SELECT
            s.codProduto ,
            s.codSortimento ,
            s.corBase as codCor
        FROM
            tcp.SortimentosProduto s
        WHERE
            s.codEmpresa = 1
            and (s.codProduto like '6%' or s.codProduto like '5%') 
            """
            with ConexaoBanco.Conexao2() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
            del rows
            gc.collect()

            return consulta

    def converterPaiemParte(self):
        '''Método utilizado para converter o reduzido do codigo PAI em reduzido Filho '''

        # 1 - Consultando os componentes de partes vinculados no PAI
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
            tcp.CompVarSorGraTam cv2 on cv2.codEmpresa = cv.codEmpresa 
            and cv2.codProduto = cv.codProduto 
            and cv.codSequencia = cv2.sequencia 
        WHERE 
            cv.codEmpresa = 1 and cv.codClassifComponente in (10,12) 
            and cv.codProduto like '%-0'
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                relacaoPartes = pd.DataFrame(rows, columns=colunas)

        relacaoPartes['codSeqTamanho'] = relacaoPartes['codSeqTamanho'].astype(str)
        relacaoPartes['codSortimento'] = relacaoPartes['codSortimento'].astype(str)

        # 1.1 - Consultando no banco do PCP o codigo reduzido do PAI relacionado ao sortimento + sequencia de tamanho
        sl2Itens2 = """
        select 
            codigo as "codItem", 
            "codSortimento"::varchar, 
            "codSeqTamanho"::varchar, '0'||"codItemPai"||'-0' as "codProduto"  
        from 
            "PCP".pcp.itens_csw ic 
        where 
            ic."codItemPai" like '1%'
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        itens = pd.read_sql(sl2Itens2, conn)

        # 2 - Obtendo o codigo reduzido da Parte para cada item PAI
        relacaoPartes = pd.merge(relacaoPartes, itens, on=['codProduto', 'codSortimento', 'codSeqTamanho'])

        # Selecionar apenas as colunas 'OP' e 'fase atual'
        relacaoPartes = relacaoPartes[['redParte', 'codItem','quantidade']]

        return relacaoPartes


    def ordemProdPCPTamCor(self):

        sql = """
        SELECT 
	        ot.codProduto ,ot.numeroop as numeroop , ot.codSortimento , seqTamanho, 
  	        case WHEN ot.qtdePecas1Qualidade is null then ot.qtdePecasProgramadas else qtdePecas1Qualidade end total_pcs 
        FROM 
	        tco.OrdemProdTamanhos ot
        WHERE 
            ot.codEmpresa = 1 and ot.numeroOP in (
                select 
                    op.numeroop 
                from tco.OrdemProd op 
                WHERE 
                    op.codempresa = 1 and op.situacao = 3 and op.codfaseatual = 401 )
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)


        return consulta


    def estoquePA(self, filtrarKIt = False):

        if filtrarKIt == True:
            sql = """
                SELECT
                    e.codItem ,
                    e.estoqueAtual,
                    i.nome,
                    i2.codCor as codCor,
                    t.descricao as tam,
                    i2.codSortimento ,
                    i2.codSeqTamanho as seqTamanho,
                    i2.codItemPai||'-0' as codProduto,
                    e.precoMedio  
                FROM
                    est.DadosEstoque e
                join 
                    cgi.Item i on i.codigo = e.codItem 
                JOIN 
                    cgi.Item2 i2 on i2.empresa = 2 and i2.coditem = i.codigo 
                JOIN 
                    tcp.Tamanhos t on t.codEmpresa = 1 and t.sequencia  = i2.codSeqTamanho 
                WHERE
                    e.codEmpresa = 1
                    and e.codNatureza = 5
                    and i.nome like 'KIT%'
            """

        else:
            sql = """
                SELECT
                    e.codItem ,
                    e.estoqueAtual,
                    i.nome,
                    i2.codCor as codCor,
                    t.descricao as tam,
                    i2.codSortimento ,
                    i2.codSeqTamanho as seqTamanho,
                    i2.codItemPai||'-0' as codProduto,
                    e.precoMedio  
                FROM
                    est.DadosEstoque e
                join 
                    cgi.Item i on i.codigo = e.codItem 
                JOIN 
                    cgi.Item2 i2 on i2.empresa = 2 and i2.coditem = i.codigo 
                JOIN 
                    tcp.Tamanhos t on t.codEmpresa = 1 and t.sequencia  = i2.codSeqTamanho 
                WHERE
                    e.codEmpresa = 1
                    and e.codNatureza = 5
                    and e.estoqueAtual > 0
            """


        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        #Obtendo o codigo do item relacionado a parte
        conversaoCodigo = self.converterPaiemParte()

        consulta = pd.merge(consulta,conversaoCodigo,on='codItem',how='left')
        consulta.fillna('-',inplace=True)


        return consulta






