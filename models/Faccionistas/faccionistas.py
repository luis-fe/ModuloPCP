import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


def ListaFaccionistasCsw():
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


def CadastrarCapacidadeDiariaFac(codFaccionista,apelido,ArrayCategorias, ArrayCapacidade):
    sql ="""SELECT * FROM pcp.faccionista """
    sql2= """SELECT * FROM pcp."faccaoCategoria" """




    return pd.DataFrame([{'Status':True,'Mensagem':'Registrado com Sucesso !'}])


def RegistroFaccionistas():
    sql = """SELECT * FROM pcp.faccionista """
    sql2 = """SELECT * FROM pcp."faccaoCategoria" """

    conn = ConexaoPostgreWms.conexaoEngine()
    sql = pd.read_sql(sql,conn)
    sql2 = pd.read_sql(sql2,conn)
    merged = pd.merge(sql, sql2, on='codfaccionista', how='left')
    merged.fillna('-',inplace=True)
    merged['nome'] = merged.apply(lambda r: r['apelidofaccionista'] if r['apelidofaccionista'] == '-' else r['nomefaccionista'],axis=1)
    # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
    grouped = merged.groupby(['codfaccionista','nome']).agg({
        'nomecategoria': lambda x: list(x.dropna().astype(str).unique()),
        'Capacidade/dia': lambda x: list(x.dropna().astype(str).unique())
    }).reset_index()

    grouped.rename(
        columns={'codfaccionista': '01- codfaccionista', 'nome': '02- nome',
                 'nomecategoria': '03- nomecategoria',
                 'Capacidade/dia': '04- Capacidade/dia'},
        inplace=True)


    return grouped


def ObterCategorias():
    sql = """Select "nomecategoria" as categoria from pcp.categoria """
    conn = ConexaoPostgreWms.conexaoEngine()
    consulta = pd.read_sql(sql,conn)

    return consulta