import gc
import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms


class GestaoPartes():
    '''Classe utilizada para fazer o gerenciamento das Chamadas partes '''
    def __init__(self, numeroOP = None, codFaseAguardandoPartes = None, codFaseMontagem = None):
        self.numeroOP = numeroOP
        self.codFaseAguardandoPartes = codFaseAguardandoPartes
        self.codFaseMontagem = codFaseMontagem
    def validarAguardandoPartesOPMae(self):
        '''Metodo que avalia se as Ops programadas que utilizam partes possue a fase aguardando Partes'''

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

        df_filtrado = df_merged[df_merged['codSeqRoteiroAtual'] < df_merged['rotMax']]


        #1.7 verificando se a OP possue a fase aguardando Partes
        agPartes = self.ordemProducaoComRoterioAguardandoPartes()
        df_filtrado = pd.merge(df_filtrado, agPartes , on='numeroOP', how='left')
        df_filtrado['possueFaseAgPartes'].fillna('Nao',inplace = True)

        # Selecionar apenas as colunas 'OP' e 'fase atual'
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
            codClassifComponente = 12
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
        and r.codfase ="""+ str(self.codFaseMontagem)+""""""


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
        and r.codfase ="""+ str(self.codFaseAguardandoPartes)+""""""


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

    def listandoPartesRelacionadas(self):
        '''Metodo utilizado para verificar as partes que estao relacionadas'''

        sql = """
        SELECT 
            codOPConjunto as numeroOP , 
            codOPParte  
        FROM 
            tco.RelacaoOPsConjuntoPartes r
        WHERE 
            r.Empresa = 1 
            and r.codOPConjunto  in (
                        SELECT 
                            o.numeroOP 
                        from
                            tco.OrdemProd o
                        WHERE 
                            o.codempresa = 1
                            AND o.situacao = 3
                            and o.numeroOP like '%-001' 
                ) 
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


    def obtendoEstoquePartesNat20(self):
        '''Metodo que obttem o estoque das partes (excluir as cuecas mantendo somente as partes da malharia)'''


        sql = """
            SELECT
                e.codItem ,
                e.estoqueAtual,
                i.nome,
                i2.codCor,
                t.descricao as tam,
                i2.codSortimento ,
	            i2.codSeqTamanho,
	            i2.codItemPai||'-0' as codProduto  
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
                and e.codNatureza = '20'
                and e.estoqueAtual > 0
                and i2.codItemPai like '6%'
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


    def detalharOPMaeGrade(self):
        '''Metodo utilizado para detalhar a OP mae a nivel de grade tamanho e cor'''

        sqlPortal = """
        select
            numeroop as "numeroOP",
            "codProduto",
            total_pcs,
            "seqTamanho" ,
            "codSortimento" 
        from
	        "PCP".pcp.ordemprod o
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sqlPortal,conn)

        validarAguardandoPartesOPMae = self.validarAguardandoPartesOPMae()

        consulta = pd.merge(validarAguardandoPartesOPMae,consulta,on='numeroOP',how='left')


        return consulta


