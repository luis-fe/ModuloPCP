import connection.ConexaoBanco as ConexaoBanco
import pandas as pd
import gc

def lote(empresa):
    sql = """SELECT * FROM TCL.lote l WHERE l.codempresa = %s"""%empresa


    with ConexaoBanco.Conexao2() as conn: 
        with conn.cursor() as cursor:
            cursor.execute(sql) 
            colunas = [desc[0] for desc in cursor.description]
            # Busca todos os dados
            rows = cursor.fetchall()
            # Cria o DataFrame com as colunas
            lotes = pd.DataFrame(rows, columns=colunas)    
            del rows
            gc.collect()
            return lotes

