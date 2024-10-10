import pandas as pd
from connection import ConexaoPostgreWms

class StatusFac():
    def __init__(self, statusTerceirizado = None):
        self.statusTerceirizado= statusTerceirizado
        self.situacaoStatus= True

    def consultarStatusDisponiveis(self):
        consulta = """
        select
	        statusterceirizado
        from
	        "PCP".pcp.statusFac
        where 
	        situacaostatus = True
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        return consulta

    def verificarExistenciaStatus(self):
        consulta = """
                select
        	        statusterceirizado, situacaostatus
                from
        	        "PCP".pcp.statusFac
                where 
        	        statusterceirizado = %s
                """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta, conn, params=(self.statusTerceirizado,))

        if consulta.empty:
            return False, False

        else:
            return consulta['statusterceirizado'][0],consulta['situacaostatus'][0]


    def cadastrarStatus(self):

        # Verifica se o status :
        status , situacao = self.verificarExistenciaStatus()

        if status != False and situacao ==False:

            self.reativarStatus()

            return pd.DataFrame(
                [{'Status': True, 'Mensagem': f'statusTerceirizado {self.statusTerceirizado} Incluido com sucesso !'}])

        elif status == False :

            inserir = """INSERT INTO "PCP".pcp.statusFac (statusterceirizado, situacaostatus) VALUES ( %s , %s )"""

            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(inserir, (self.statusTerceirizado, self.situacaoStatus))
                    connInsert.commit()

            return pd.DataFrame(
                [{'Status': True, 'Mensagem': f'statusTerceirizado {self.statusTerceirizado} Incluido com sucesso !'}])

        else:
            self.editarStatus(self.statusTerceirizado)
            return pd.DataFrame(
                [{'Status': True, 'Mensagem': f'statusTerceirizado {self.statusTerceirizado} Incluido com sucesso !'}])



    def editarStatus(self, novos_tatusterceirizado):

        update = """
        UPDATE 
            "PCP".pcp.statusFac
        SET 
            statusterceirizado = %s
        where 
            statusterceirizado = %s
        """

        with ConexaoPostgreWms.conexaoInsercao() as connInsert:
            with connInsert.cursor() as curr:
                curr.execute(update, (novos_tatusterceirizado, self.situacaoStatus))
                connInsert.commit()

        return pd.DataFrame(
            [{'Status': True, 'Mensagem': f'statusTerceirizado {novos_tatusterceirizado} renomeado com sucesso !'}])


    def excluirStatus(self):
        update = """
        UPDATE 
            "PCP".pcp.statusFac
        SET 
            situacaoStatus = False
        where 
            statusterceirizado = %s
        """

        with ConexaoPostgreWms.conexaoInsercao() as connInsert:
            with connInsert.cursor() as curr:
                curr.execute(update, (self.statusTerceirizado, False))
                connInsert.commit()

        return pd.DataFrame(
            [{'Status': True, 'Mensagem': f'statusTerceirizado {self.statusTerceirizado} removido com sucesso !'}])


    def reativarStatus(self):

        update = """
        UPDATE 
            "PCP".pcp.statusFac
        SET 
            situacaoStatus = False
        where 
            statusterceirizado = %s
        """

        with ConexaoPostgreWms.conexaoInsercao() as connInsert:
            with connInsert.cursor() as curr:
                curr.execute(update, (self.statusTerceirizado, True))
                connInsert.commit()

        return pd.DataFrame(
            [{'Status': True, 'Mensagem': f'statusTerceirizado {self.statusTerceirizado} revertido com sucesso !'}])
