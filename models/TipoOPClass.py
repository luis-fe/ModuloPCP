import gc
import pandas as pd
from connection import ConexaoBanco


class TipoOP():
    '''Class referente ao tipo de OP '''
    def __init__(self, codTipoOP = None):
        self.codTipoOP = codTipoOP

    def obterTodosTipos(self):
        '''Metodo qe busca no ERP CSW todos os tipo de OPS'''

        sql = """
            SELECT
        	t.codTipo || '-' || t.nome as tipoOP
        FROM
        	tcp.TipoOP t
        WHERE
        	t.Empresa = 1 and t.codTipo not in (7, 13, 14, 15, 19, 21, 23, 61,24,25,26, 11, 20, 28)
        order by
        	codTipo asc
            """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                tipoOP = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return tipoOP

    def tiposDeProducaoAgrupado(self):
        '''Metodo qe agrupa o tipo de OP em categorias'''
        print("Executando tiposDeProducaoAgrupado")
        categorias = pd.DataFrame({'Categoria': ['Producao', 'Mostruario', 'Encomendas','Varejo']})
        return categorias


    def agrupado_x_tipoOP(self):
        '''metodo que compara o agrupado x tipo de op '''


        tipoOP = self.obterTodosTipos()
        tipoOP.loc[tipoOP['tipoOP'] == '1-PRODUTO VENDA', 'Agrupado'] = 'Producao'
        tipoOP.loc[tipoOP["tipoOP"].str.contains("VAREJO", na=False), "Agrupado"] = "Varejo"
        tipoOP.loc[tipoOP["tipoOP"].str.contains("2-", na=False), "Agrupado"] = "Producao"
        tipoOP.loc[tipoOP["tipoOP"].str.contains("ENC", na=False), "Agrupado"] = "Encomendas"
        tipoOP.loc[tipoOP["tipoOP"].str.contains("MOST", na=False), "Agrupado"] = "Mostruario"
        tipoOP.fillna('Producao',inplace=True)

        return tipoOP



