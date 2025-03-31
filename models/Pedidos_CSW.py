import pandas as pd
from connection import ConexaoBanco


class Pedidos_CSW():
    '''Classe utilizado para interagir com os Pedidos do Csw '''

    def __init__(self, codEmpresa = '1'):
        '''Construtor da classe '''

        self.codEmpresa = codEmpresa # codEmpresa


    def pedidosBloqueados(self):
        '''Metodo que pesquisa no Csw os pedidos bloqueados '''


        consultacsw = """
        SELECT 
            * 
        FROM 
            (
                SELECT top 300000 
                    bc.codPedido, 
                    'analise comercial' as situacaobloq  
                from 
                    ped.PedidoBloqComl  bc 
                WHERE 
                    codEmpresa = 1  
                    and bc.situacaoBloq = 1
                order by 
                    codPedido desc
                UNION 
                SELECT top 300000 
                    codPedido, 
                    'analise credito'as situacaobloq  
                FROM 
                    Cre.PedidoCreditoBloq 
                WHERE 
                    Empresa  = 1  
                    and situacao = 1
                order BY 
                    codPedido DESC
            ) as D"""

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultacsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            del rows
            return consulta