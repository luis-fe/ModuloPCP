import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


class Faccionista():
    def __init__(self,codfaccionista = None, apelidofaccionista= None, nomecategoria = None, Capacidade_dia = None ):
        self.codfaccionista = codfaccionista
        self.nomefaccionista = None
        self.apelidofaccionista = apelidofaccionista
        self.nomecategoria = nomecategoria
        self.Capacidade_dia = Capacidade_dia
        self.conn = ConexaoPostgreWms.conexaoEngine() # Conexao com o banco de dados

    def ObterNomeCSW(self):
        sql = """SELECT
        	f.codFaccionista ,
        	f.nome as nomeFaccionista
        FROM
        	tcg.Faccionista f
        WHERE
        	f.Empresa = 1 order by nome """
        with ConexaoBanco.Conexao2() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        consulta = consulta[consulta['codFaccionista']==int(self.codfaccionista)].reset_index()
        self.nomeFaccionista = consulta['nomeFaccionista'][0]
        return self.nomeFaccionista

    def ConsultarCategoriaMetaFaccionista(self):
            select = """select
	        fc.codfaccionista ,
	        fc.nomecategoria ,
	        fc."Capacidade/dia"
            from
	        "PCP".pcp."faccaoCategoria" fc
    	 WHERE codfaccionista = %s
    	 """
            consulta = pd.read_sql(select, self.conn, params=(self.codfaccionista,))

            return consulta
    def ConsultarFaccionista(self):
        select = """select
	        f.codfaccionista ,
	        f.nomefaccionista ,
	        f.apelidofaccionista 
        from
	        "PCP".pcp.faccionista f
            WHERE codfaccionista = %s
            	 """

        consulta = pd.read_sql(select, self.conn, params=(self.codfaccionista,))
        if consulta.empty:
            consulta['Status'] = False

        consulta['Status'] = True

        return consulta

    def AlterarFaccionista(self):
        consultarCategoriaMeta = self.ConsultarCategoriaMetaFaccionista()
        VerificarCategoria = consultarCategoriaMeta[consultarCategoriaMeta['nomecategoria']==self.nomecategoria].reset_index()


        if VerificarCategoria.empty:
            inserirCategoria ="""INSERT INTO "PCP".pcp."faccaoCategoria" ("Capacidade/dia" ,nomecategoria, codfaccionista ) values (%s, %s, %s)
            """
            updateFaccionista = """UPDATE "PCP".pcp.faccionista SET apelidofaccionista = %s WHERE codfaccionista = %s """

            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(inserirCategoria,(self.Capacidade_dia,self.nomecategoria,self.codfaccionista))
                    connInsert.commit()
                    curr.execute(updateFaccionista,(self.apelidofaccionista,self.codfaccionista))
                    connInsert.commit()

        else:
            update ="""UPDATE "PCP".pcp."faccaoCategoria" 
            SET "Capacidade/dia" = %s , nomecategoria = %s
            where codfaccionista = %s and nomecategoria = %s
            """

            updateFaccionista = """UPDATE "PCP".pcp.faccionista SET apelidofaccionista = %s WHERE codfaccionista = %s """

            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(update,(self.Capacidade_dia,self.nomecategoria,self.codfaccionista,self.nomecategoria))
                    connInsert.commit()
                    curr.execute(updateFaccionista,(self.apelidofaccionista,self.codfaccionista))
                    connInsert.commit()


        return pd.DataFrame([{'Status':True,'Mensagem':f'Faccionista {self.codfaccionista} alterado com sucesso !'}])

    def InserirFaccionista(self):
        VerificaFaccionista = self.ConsultarFaccionista()
        if VerificaFaccionista['Status'][0] == False:
            insert = """insert into "PCP".pcp.faccionista (codfaccionista, nomefaccionista, apelidofaccionista ) values (%s, %s, %s )"""
            insert2 = """insert into "PCP".pcp."faccaoCategoria" (codfaccionista, nomecategoria, "Capacidade/dia" ) values (%s, %s, %s) """
            self.nomefaccionista = self.ObterNomeCSW()

            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(insert, (self.codfaccionista, self.nomefaccionista, self.apelidofaccionista))
                    connInsert.commit()
                    curr.execute(insert2, (self.codfaccionista, self.nomecategoria,self.Capacidade_dia))
                    connInsert.commit()


            return pd.DataFrame(
                [{'Status': True, 'Mensagem': f'Faccionista {self.codfaccionista} Incluido com sucesso !'}])

        else:
            alterar = self.AlterarFaccionista()
            return alterar

    def ExcluirFaccinonistaCategoria(self):
        VerificaFaccionista = self.ConsultarFaccionista()
        if VerificaFaccionista['Status'] == False:
            return pd.DataFrame({'Status':False,'Mensagem':'Faccionista nao encontrado'})
        else:
            deleteCategoria = """DELETE FROM "PCP".pcp."faccaoCategoria" where nomecategoria = %s and codfaccionista = %s """
            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(deleteCategoria, (self.nomecategoria, self.codfaccionista))
                    connInsert.commit()
            verificarFacCategoria = self.ConsultarCategoriaMetaFaccionista()
            if verificarFacCategoria.empty:
                deleteFaccionista = """DELETE FROM "PCP".pcp.faccionista WHERE codfaccionista = %s"""
                with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                    with connInsert.cursor() as curr:
                        curr.execute(deleteFaccionista, (self.codfaccionista))
                        connInsert.commit()

            return pd.DataFrame([{'Status':True,'Mensagem':'Faccionista/categoria excluido com sucesso!'}])
