from connection import ConexaoPostgreWms,ConexaoBanco
import pandas as pd


class FaseProducao():
    '''Clase para  as informacoes de Fase de Producao'''

    def __init__(self, codFase = None, responsavel = None, leadTimeMeta = None, nomeFase = None):
        '''Construtor da clase
        params:
        codFase - opcional,
        responsavel -opcional,
        lead time -opcional,
        nomeFase - opcional

        caso nao passe parametros assume-se que as informacoes sao para faccionistas (s);
        '''
        self.codFase = codFase
        self.responsavel = responsavel
        self.leadTimeMeta = leadTimeMeta

        # Encontrando o nome da fase no ERP CSW
        if nomeFase == None and codFase != None:
            sqlCsw = """
                    SELECT
                        f.codFase ,
                        f.nome as nomeFase
                    FROM
                        tcp.FasesProducao f
                    WHERE
                        f.codEmpresa = 1
                    and f.codFase = """+str(self.codFase)

            with ConexaoBanco.Conexao2() as connCsw:
                with connCsw.cursor() as cursor:
                    cursor.execute(sqlCsw)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    consulta = pd.DataFrame(rows, columns=colunas)
            if consulta.empty:
                self.nomeFase = '-'
            else:
                self.nomeFase = consulta['nomeFase'][0]
        else:
            self.nomeFase = nomeFase


    def InserirMetaLT_Responsavel(self):
        insert = """
        insert into pcp."responsabilidadeFase"  ("codFase" , responsavel, "metaLeadTime") values (%s , %s, %s) 
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(insert,(str(self.codFase), self.responsavel, int(self.leadTimeMeta)))
                conn.commit()

        return pd.DataFrame([{'status':True, 'Mensagem':f'Responsavel e Lead Time inserirdos com sucesso na fase {self.codFase}-{self.nomeFase}'}])

    def ConsultarFases(self):
        consulta = """
        select "codFase" , responsavel, "metaLeadTime" from pcp."responsabilidadeFase" rf
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta,conn)
        return consulta

    def UpdateMetaLT_Responsavel(self):
        update = """UPDATE pcp."responsabilidadeFase" 
        SET responsavel = %s , "metaLeadTime"= %s
        where "codFase" = %s
        """
        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(update,(self.responsavel, self.leadTimeMeta,self.codFase,))
                conn.commit()

        return pd.DataFrame([{'status':True, 'Mensagem':f'Responsavel e Lead Time alterados com sucesso na fase {self.codFase}-{self.nomeFase}'}])

    def AlterarMetaLT_Responsave(self):

        pesquisa = self.ConsultarFases()
        pesquisa = pesquisa[pesquisa['codFase'] ==self.codFase]

        if pesquisa.empty:
            alteracao =self.InserirMetaLT_Responsavel()
            return alteracao
        else:
            alteracao = self.UpdateMetaLT_Responsavel()
            return alteracao

    def ConsultaFasesProdutivaCSWxGestao(self):

        sqlCsw = """
                SELECT
                    f.codFase ,
                    f.nome as nomeFase
                FROM
                    tcp.FasesProducao f
                WHERE
                    f.codEmpresa = 1
                and f.codFase > 400 and f.codFase < 599
                order by codFase asc
                """


        with ConexaoBanco.Conexao2() as connCsw:
            with connCsw.cursor() as cursor:
                cursor.execute(sqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        consultaFasesGestao = self.ConsultarFases()
        consulta['codFase'] = consulta['codFase'].astype(str)
        consulta = pd.merge(consulta,consultaFasesGestao,on='codFase',how='left')
        consulta.fillna('-',inplace=True)
        return consulta

