import gc
import pandas as pd
from connection import ConexaoBanco


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
        df_merged = pd.merge(opProgramada, opsProgramadaFaseMontagem, on='OP')
        df_filtrado = df_merged[df_merged['codSeqRoteiroAtual'] < df_merged['rotMax']]

        # Selecionar apenas as colunas 'OP' e 'fase atual'
        resultado = df_filtrado[['numeroOP', 'codSeqRoteiroAtual']]

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
