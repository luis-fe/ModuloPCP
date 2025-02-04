


class  OrdemProd():
        '''classe criada para a gestao de OPs da Producao '''

        def __init__(self, codEmpresa = None):

            self.codEmpresa = codEmpresa



        def parquetOrdemProducao(self):
            '''Metodo criado para gerar um arquivo parquet com as OPs'''


            sql = """
            select * from "PCP".pcp.ordemprod o 
            """


