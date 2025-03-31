import gc

import pandas as pd
from connection import ConexaoBanco


class OP_CSW():
    '''Classe utilizada para interagir com informacoes de OP do Csw'''

    def __init__(self, codEmpresa = '1', codLote = ''):
        '''Construtor'''

        self.codEmpresa = str(codEmpresa) # atributo de codEmpresa
        self.codLote = codLote # atributo com o codLote

    def ordemProd_csw_aberto(self):
        ''' metodo utilizado para obter no csw as ops em aberto'''

        # 1: Carregar o SQL das OPS em aberto do CSW
        sqlOrdemAbertoCsw = """
            SELECT 
                op.codLote , 
                codTipoOP , 
                numeroOP, 
                codSeqRoteiroAtual, 
                lot.descricao as desLote, 
                codfaseatual 
            from 
                tco.OrdemProd op 
            inner join 
                tcl.Lote lot 
                on lot.codLote = op.codLote  
                and lot.codEmpresa  = """+self.codEmpresa+""" 
            WHERE
                op.codempresa = """+self.codEmpresa+""" 
                and op.situacao = 3 """


        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlOrdemAbertoCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows

        return consulta


    def roteiro_ordemProd_csw_aberto(self):
        ''' metodo utilizado para obter no csw o roteiro das ops em aberto'''

        sqlCsw = """
            SELECT
                numeroOP ,
                codSeqRoteiro,
                codFase,
                (
                SELECT
                    codtipoop
                from
                    tco.OrdemProd o
                WHERE
                    o.codempresa = """+self.codEmpresa+""" 
                    and o.numeroop = r.numeroOP
                ) as tipoOP
            FROM
                tco.RoteiroOP r
            WHERE
                r.codEmpresa = """+self.codEmpresa+""" 
                and 
            numeroOP in (
                SELECT
                    numeroOP
                from
                    tco.OrdemProd op
                WHERE
                    op.codempresa = """+self.codEmpresa+""" 
                    and op.situacao = 3
                    and op.codFaseAtual not in (1, 401))
        """

        with ConexaoBanco.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows

        return consulta

    def consultarLoteEspecificoCsw(self):
        '''Método que consulta o codigo do lote no CSW e retorna o seu nome no CSW'''


        sql = """Select codLote, descricao as nomeLote from tcl.lote where codEmpresa= """ + str(
            self.codEmpresa) + """ and codLote =""" + "'" + str(self.codLote) + "'"

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        nomeLote = lotes['nomeLote'][0]
        nomeLote = nomeLote[:2] + '-' + nomeLote

        return nomeLote


    def informacoesFasesCsw(self):
        '''Método que consulta no csw as informacoes das fases cadastradas no CSW'''


        sql_nomeFases = """
        SELECT 
            f.codFase , 
            f.nome as fase 
        FROM 
            tcp.FasesProducao f
        WHERE 
            f.codEmpresa = 1 
        """


        return sql_nomeFases





