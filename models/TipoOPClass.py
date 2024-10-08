import gc
import pandas as pd
from connection import ConexaoBanco


class TipoOP():
    def __init__(self, codTipoOP = None):
        self.codTipoOP = codTipoOP

    def obiterTodosTipos(self):
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