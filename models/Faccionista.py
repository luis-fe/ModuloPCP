import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
from models import MetaFaccionistaClass

class Faccionista():
    '''Classe Faccionista: definida para instanciar o objeto faccionista ou faccionista(s)'''

    def __init__(self, codFaccionsita = None, nomeFaccionista = None , apelidoFaccionsita = None):
        self.codFaccionista = codFaccionsita
        self.nomeFaccionsita = nomeFaccionista
        self.apelidoFaccionista = apelidoFaccionsita

    def consultarFaccionista(self):
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
            conn = ConexaoPostgreWms.conexaoEngine()

            if self.codFaccionista == None:
                consulta = pd.read_sql(selectAll, conn)
                consulta['Status'] = True
            else:
                consulta = pd.read_sql(select, conn, params=(str(self.codFaccionista),))

                if consulta.empty:
                    consulta['Status'] = False
                    consulta = pd.DataFrame([{'Status': False}])
                else:
                    consulta['Status'] = True

            return consulta

    def cadastrarFaccionsita(self):
        VerificaFaccionista = self.consultarFaccionista()
        if VerificaFaccionista['Status'][0] == False:
            insert = """insert into "PCP".pcp.faccionista (codfaccionista, nomefaccionista, apelidofaccionista ) values (%s, %s, %s )"""
            self.nomefaccionista = self.obternomeFaccCsw()

            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(insert, (self.codFaccionista, self.nomefaccionista, self.apelidoFaccionista))
                    connInsert.commit()

            return pd.DataFrame(
                [{'Status': True, 'Mensagem': f'Faccionista {self.codFaccionista} Incluido com sucesso !'}])

        else:
            alterar = self.editarFaccionista()
            return alterar

    def obternomeFaccCsw(self):
        '''Metodo  para obter nome dos faccionistas no csw
        return:
        string: self.nomeFaccionista  - identifica qual o nome o faccionista no csw de acordo com o sef.codFaccionista
        '''

        # 1 - SQL
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

        consulta = consulta[consulta['codFaccionista']==int(self.codFaccionista)].reset_index()
        self.nomeFaccionista = consulta['nomeFaccionista'][0]

        return self.nomeFaccionista


    def editarFaccionista(self):
        '''Metodo que altera ou inserir faccionista novo
                return:
                DataFrame: [{Status da resposta boolean }]
        '''
        updateFaccionista = """UPDATE "PCP".pcp.faccionista SET apelidofaccionista = %s WHERE codfaccionista::varchar = %s """
        with ConexaoPostgreWms.conexaoInsercao() as connInsert:
            with connInsert.cursor() as curr:
                curr.execute(updateFaccionista, (self.apelidoFaccionista, str(self.codFaccionista)))
                connInsert.commit()

        return pd.DataFrame(
                [{'Status': True, 'Mensagem': f'Faccionista {self.codFaccionista} alterado com sucesso !'}])

    def obterCodigosFaccionista(self):

        consulta = '''
        select
	        f.codfaccionista 
        from
	        "PCP".pcp.faccionista  f 
	    where
	        f.nomefaccionista = %s
        '''

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta,conn, params=(self.nomeFaccionsita))

        return consulta

    def ListaFaccionistasCsw(self):
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

        return consulta
    def RegistroFaccionistas(self):
        '''Metodo que retorna todas as informacoes de registro de faccionista, cadastrado no portal'''


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



