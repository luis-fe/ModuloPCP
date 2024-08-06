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
    inserir1 ="""insert into pcp.faccionista  ("codfaccionista","apelidofaccionista", "nomefaccionista") values ( %s , %s, %s ) """
    inserir2= """insert into pcp."faccaoCategoria" ("codfaccionista", "nomecategoria", "Capacidade/dia")  values ( %s , %s, %s) """
    sql = """SELECT * FROM pcp.faccionista where "codfaccionista"= %s """
    sql2 = """SELECT * FROM pcp."faccaoCategoria" where "codfaccionista"= %s and nomecategoria = %s """

    conn1 = ConexaoPostgreWms.conexaoEngine()



    with ConexaoPostgreWms.conexaoInsercao() as conn:
        nomefaccionista = ObterNomeCSW(int(codFaccionista))
        sql = pd.read_sql(sql,conn1,params=(str(codFaccionista),))
        if sql.empty:

            with conn.cursor() as curr:
                curr.execute(inserir1,(codFaccionista,apelido,nomefaccionista))
                conn.commit()

        else:
            update = """update pcp.faccionista set  "apelidofaccionista" = %s, "nomefaccionista" = %s where "codfaccionista" = %s """
            with conn.cursor() as curr:
                curr.execute(update,(apelido, nomefaccionista, codFaccionista))
                conn.commit()

        for categoria, capacidade in zip(ArrayCategorias, ArrayCapacidade):
            sql2 = pd.read_sql(sql2, conn1,params=(codFaccionista, categoria))

            if sql2.empty:
                with conn.cursor() as curr:
                    curr.execute(inserir2, (codFaccionista, categoria, capacidade))
                    conn.commit()
            else:
                update = """UPDATE  pcp."faccaoCategoria" set nomecategoria = %s, "Capacidade/dia" = %s where "codfaccionista"= %s and nomecategoria = %s    """

                with conn.cursor() as curr:
                    curr.execute(update, (categoria,capacidade,codFaccionista,categoria))
                    conn.commit()

    return pd.DataFrame([{'Status':True,'Mensagem':'Registrado com Sucesso !'}])


def RegistroFaccionistas():
    sql = """SELECT * FROM pcp.faccionista """
    sql2 = """SELECT * FROM pcp."faccaoCategoria" """

    conn = ConexaoPostgreWms.conexaoEngine()
    sql = pd.read_sql(sql,conn)
    sql2 = pd.read_sql(sql2,conn)
    merged = pd.merge(sql, sql2, on='codfaccionista', how='left')
    merged.fillna('-',inplace=True)
    merged['nome'] = merged.apply(lambda r: r['apelidofaccionista'] if r['apelidofaccionista'] != '-' else r['nomefaccionista'],axis=1)
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

def ObterNomeCSW(codfaccionista):
    consulta = ListaFaccionistasCsw()
    consulta = consulta[consulta['codFaccionista']==int(codfaccionista)].reset_index()

    return consulta['nomeFaccionista'][0]