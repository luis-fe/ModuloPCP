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
	        fc."Capacidade/dia"::int
            from
	        "PCP".pcp."faccaoCategoria" fc
    	 WHERE codfaccionista = %s
    	 """
            consulta = pd.read_sql(select, self.conn, params=(str(self.codfaccionista),))

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

        selectAll = """select
        	        f.codfaccionista ,
        	        f.nomefaccionista ,
        	        f.apelidofaccionista 
                from
        	        "PCP".pcp.faccionista f
                    	 """

        if self.codfaccionista == None:
            consulta = pd.read_sql(selectAll, self.conn)
            consulta['Status'] = True
        else:
            consulta = pd.read_sql(select, self.conn, params=(str(self.codfaccionista),))
            if consulta.empty:
                consulta['Status'] = False
                consulta = pd.DataFrame([{'Status':False}])
            else:
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

    def RegistroFaccionistas(self):
        sql = """SELECT * FROM pcp.faccionista """
        sql2 = """SELECT * FROM pcp."faccaoCategoria" """
        sql3_Csw ="""
        SELECT
        	f.codFaccionista as codfaccionista ,
        	f.nome as nomeFaccionistaCsw
        FROM
        	tcg.Faccionista f
        WHERE
        	f.Empresa = 1 order by nome """

        with ConexaoBanco.Conexao2() as connCsw:
                with connCsw.cursor() as cursor:
                    cursor.execute(sql3_Csw)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    sql3_Csw = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        conn = ConexaoPostgreWms.conexaoEngine()
        sql = pd.read_sql(sql, conn)
        sql2 = pd.read_sql(sql2, conn)
        merged = pd.merge(sql, sql2, on='codfaccionista', how='left')
        merged.fillna('-', inplace=True)
        merged['nome'] = merged.apply(
            lambda r: r['apelidofaccionista'] if r['apelidofaccionista'] != '-' else r['nomefaccionista'], axis=1)
        # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
        grouped = merged.groupby(['codfaccionista', 'nome']).agg({
            'nomecategoria': lambda x: list(x.dropna().astype(str).unique()),
            'Capacidade/dia': lambda x: list(x.dropna().astype(str).unique())
        }).reset_index()
        sql3_Csw['codfaccionista'] = sql3_Csw['codfaccionista'].astype(str)
        grouped = pd.merge(grouped, sql3_Csw, on='codfaccionista',how='left')
        grouped.rename(
            columns={'codfaccionista': '01- codfaccionista', 'nome': '02- nome',
                     'nomecategoria': '03- nomecategoria',
                     'Capacidade/dia': '04- Capacidade/dia'},
            inplace=True)



        return grouped
