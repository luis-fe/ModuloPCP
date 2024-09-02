import gc
import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
import pytz


class ProducaoRealizado():
    '''Classe especifica para gerenciar as informacoes do REALIZADO DA PRODUCAO'''

    def __init__(self, empresa = None):
        '''Contrutor da classe '''
        self.empresa = empresa

    def getRealizadoCSW(self,utimosDias):
        '''Funcao especifica para buscar no ERP CSW O REALIZANDO DOS ULTIMOS N DIAS,
                ONDE A VARIAVEL ultimosDias define o N dias'''


        #Funcao sql :
        sql = """SELECT f.numeroop as numeroop, f.codfase as codfase, f.seqroteiro, f.databaixa, 
         f.nomeFaccionista, f.codFaccionista,
         f.horaMov, f.totPecasOPBaixadas, 
         f.descOperMov, (select op.codProduto  from tco.ordemprod op WHERE op.codempresa = 1 and op.numeroop = f.numeroop) as codEngenharia,
           (select op.codTipoOP  from tco.ordemprod op WHERE op.codempresa = 1 and op.numeroop = f.numeroop) as codtipoop,
                      (select l.descricao  from tco.ordemprod op inner join tcl.Lote l on l.codEmpresa = 1 and l.codLote = op.codLote 
                  WHERE op.codempresa = 1 and op.numeroop = f.numeroop) as descricaoLote
           FROM tco.MovimentacaoOPFase f
         WHERE f.codEmpresa = 1 and f.databaixa >=  DATEADD(DAY, -""" + str(utimosDias) + """, GETDATE())"""

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                dataFrame_RealizadoCSW = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return dataFrame_RealizadoCSW

    def InserirDadosNoPCP(self,ulimosDias,limit):
        '''Funcao que realizaca a insercao dos dados novos no banco de dados do PCP'''

        self.deletar_15diasRealizado() # Deleta os ultimos 15 do banco PCP para corrigir duplicacoes
        verifica = self.consultarStatausRealizadoBancoPCP(limit) # Verifica quais saos as OP||Fase que existe no banco do PCP , limitado a "N" dias para ganhar performace
        sql = self.getRealizadoCSW(ulimosDias) # Busca no CSW o Realizado das Fases nos ultimosDias de acordo com o parametro


        #### Descobre o que ainda nao tem no Banco PCP e faz o incremento #####

        sql['chave'] = sql['numeroop'] + '||' + sql['codfase'].astype(str)
        sql = pd.merge(sql, verifica, on='chave', how='left')
        sql['status'].fillna('-', inplace=True)
        sql = sql[sql['status'] == '-'].reset_index()
        sql = sql.drop(columns=['status', 'index'])

        if sql['numeroop'].size > 0:
            # Implantando no banco de dados do Pcp
            ConexaoPostgreWms.Funcao_InserirOFF(sql, sql['numeroop'].size, 'realizado_fase', 'append')
        else:
            print('segue o baile')

    def consultarStatausRealizadoBancoPCP(self, limit):
        sqlPCP = """
        select distinct CHAVE, 'ok' as status from "PCP".pcp.realizado_fase
        order by CHAVE desc limit %s
        """
        conn = ConexaoPostgreWms.conexaoEngine()
        verifica = pd.read_sql(sqlPCP, conn, params=(limit,))
        return verifica

    def deletar_15diasRealizado(self):
        sqlDelete = """
        delete from "PCP".pcp.realizado_fase 
        where "dataBaixa"::Date >=  CURRENT_DATE - INTERVAL '15 days'; 
        """
        conn1 = ConexaoPostgreWms.conexaoInsercao()
        curr = conn1.cursor()
        curr.execute(sqlDelete, )
        conn1.commit()
        curr.close()
        conn1.close()