from models import Faccionista as Facc
from connection import ConexaoPostgreWms
import pandas as pd

class FaccionistaCategoria():
    '''Classe FaccionistaCategoria '''

    def __init__(self, codFaccionsita = None, nomeCategoria = None,  capacidade = None, leadTime = None ):
        faccionsita = Facc.Faccionista(codFaccionsita)
        self.codFaccionista = faccionsita.codFaccionista
        self.nomeCategoria = nomeCategoria
        self.capacidade = capacidade
        self.leadTime = leadTime
        # Caso o LeadTime for None , busca da Categoria:
        if self.leadTime == None:

            select = """
                    select
        	            leadtime
                            from
                                "PCP".pcp.leadtime_categorias lc
                            where
                                codfase = '429' and categoria = %s
                    """

            conn = ConexaoPostgreWms.conexaoEngine()
            print(f'nomeCategoria{self.nomeCategoria}')
            leadTimeCategoria = pd.read_sql(select, conn, params=(self.nomeCategoria,))

            if leadTimeCategoria.empty:
                self.leadTime = 0
            else:
                self.leadTime = leadTimeCategoria['leadtime'][0]

    def incluirFaccCategoria(self):



        inserirCategoria = """INSERT INTO "PCP".pcp."faccaoCategoria" ("Capacidade/dia" ,nomecategoria, codfaccionista, "leadTime" ) values (%s, %s, %s, %s)
                    """

        with ConexaoPostgreWms.conexaoInsercao() as connInsert:
            with connInsert.cursor() as curr:
                curr.execute(inserirCategoria, (self.capacidade, self.nomeCategoria, self.codFaccionista, self.leadTime))
                connInsert.commit()

        return pd.DataFrame(
            [{'Status': True, 'Mensagem': f'Faccionista {self.codfaccionista} Incluido com sucesso !'}])


    def editarFaccCategoria(self):

        update = """UPDATE "PCP".pcp."faccaoCategoria" 
        SET "Capacidade/dia" = %s , nomecategoria = %s, "leadTime" = %s
        where codfaccionista::varchar = %s and nomecategoria = %s
        """
        with ConexaoPostgreWms.conexaoInsercao() as connInsert:
            with connInsert.cursor() as curr:
                curr.execute(update, (self.capacidade, self.nomeCategoria, self.leadTime, self.codFaccionista, self.nomeCategoria))
                connInsert.commit()
    def obterFaccionistasCategoria(self):

        select = """
            select 
                apelidofaccionista,
                nomecategoria as categoria,
                fc.codfaccionista as codfaccionista,
                case when fc."leadTime" is null then lc.leadtime::int else fc."leadTime" end "leadTime" 
            from
                 pcp."faccaoCategoria" fc
            inner join 
                pcp."faccionista" f on 
                f.codfaccionista = fc.codfaccionista
            left join
                pcp.leadtime_categorias lc on 
                lc.categoria = fc.nomecategoria and lc.codFase = '429'
            where
                nomecategoria = %s;
        """


        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select, conn,params=(self.nomeCategoria,))

        return consulta



    def obterFaccionistasCategoriaPorFac(self):

        select = """
                        select 
                apelidofaccionista,
                nomecategoria as categoria,
                fc.codfaccionista as codfaccionista,
                case when fc."leadTime" is null then lc.leadtime::int else fc."leadTime" end "leadTime" 
            from
                 pcp."faccaoCategoria" fc
            inner join 
                pcp."faccionista" f on 
                f.codfaccionista = fc.codfaccionista
            left join
                pcp.leadtime_categorias lc on 
                lc.categoria = fc.nomecategoria and lc.codFase = '429'
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select, conn)

        return consulta

