import gc

import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


def AnaliseDeMateriais(codPlano, codLote, congelado):
    conn = ConexaoPostgreWms.conexaoEngine()
    if congelado == False:

        # Obtendo as Previsao do Lote
        sqlMetas = """
        SELECT "codLote", "Empresa", "codEngenharia", "codSeqTamanho", "codSortimento", previsao
        FROM "PCP".pcp.lote_itens li
        WHERE "codLote" = %s
        """
        sqlMetas = pd.read_sql(sqlMetas,conn, params=(codLote,))

        #Obtendo os consumos de todos os componentes relacionados nas engenharias
        sqlcsw = """
        SELECT v.codProduto as codEngenharia, cv.codSortimento, cv.seqTamanho as codSeqTamanho,  v.CodComponente,
        (SELECT i.nome FROM cgi.Item i WHERE i.codigo = v.CodComponente) as descricaoComponente,
        cv.quantidade  from tcp.ComponentesVariaveis v 
        join tcp.CompVarSorGraTam cv on cv.codEmpresa = v.codEmpresa and cv.codProduto = v.codProduto and cv.sequencia = v.codSequencia 
        WHERE v.codEmpresa = 1
        and v.codProduto in (select l.codengenharia from tcl.LoteSeqTamanho l WHERE l.empresa = 1 and l.codlote = '"""+codLote+"""')
        UNION 
        SELECT v.codProduto as codEngenharia,  l.codSortimento ,l.codSeqTamanho as codSeqTamanho, v.CodComponente,
        (SELECT i.nome FROM cgi.Item i WHERE  i.codigo = v.CodComponente) as descricaoComponente,
        v.quantidade  from tcp.ComponentesPadroes  v 
        join tcl.LoteSeqTamanho l on l.Empresa = v.codEmpresa and l.codEngenharia = v.codProduto and l.codlote = '"""+codLote+"""'"""

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlcsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consumo = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        sqlMetas = pd.merge(sqlMetas, consumo, on=["codEngenharia" , "codSeqTamanho" , "codSortimento"], how='left')
        sqlMetas['quantidade'].fillna(0,inplace=True)
        sqlMetas['quantidade Prevista'] = sqlMetas['quantidade'] * sqlMetas['previsao']
        sqlMetas = sqlMetas.groupby(["CodComponente"]).agg({"descricaoComponente":"first","quantidade Prevista":"sum"}).reset_index()


        return sqlMetas
