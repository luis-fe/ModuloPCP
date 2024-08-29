import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


class Lote():
    def __init__(self,codLote = None, codEmpresa = '1'):
        self.codLote = codLote
        self.codEmpresa = codEmpresa


    def PrevisaoLote(self):
        conn = ConexaoPostgreWms.conexaoEngine()
        # Obtendo as Previsao do Lote
        sqlMetas = """
                    select
                        "codLote",
                        "Empresa",
                        li."codEngenharia",
                        li."codSeqTamanho"::varchar,
                        li."codSortimento"::varchar,
                        previsao,
                        datas."codItem",
                        "unidadeMedida",
                        nome
                    from
                        "PCP".pcp.lote_itens li	
                    inner join (
                        select
                        codigo AS "codItem",
                        nome,
                        "unidadeMedida",
                        CASE 
                            WHEN SUBSTRING("codItemPai", 1, 1) = '1' THEN '0' || "codItemPai" || '-0' 
                            WHEN SUBSTRING("codItemPai", 1, 1) = '2' THEN '0' || "codItemPai" || '-0' 
                            WHEN SUBSTRING("codItemPai", 1, 1) = '3' THEN '0' || "codItemPai" || '-0' 
                            ELSE "codItemPai" || '-0' 
                        END AS "codEngenharia",
                            ic."codSortimento"::varchar AS "codSortimento",
                            ic."codSeqTamanho"::varchar AS "codSeqTamanho"
                    FROM
                        pcp.itens_csw ic) as datas 
                    on datas."codEngenharia" = li."codEngenharia" 
                        and datas."codSortimento"::varchar = li."codSortimento"::varchar
                        and datas."codSeqTamanho"::varchar = li."codSeqTamanho"::varchar
        WHERE "codLote" = %s
        """
        sqlMetas = pd.read_sql(sqlMetas, conn, params=(self.codLote,))

        return sqlMetas


    def loteCsw(self):
        """
        Get dos Lotes cadastrados no CSW para PREVISAO.
        """

        if self.codEmpresa == '1':
            sql = """ SELECT 
                        codLote, 
                        descricao as nomeLote 
                    FROM tcl.Lote  l
                    WHERE 
                    l.descricao like '%PREV%' and l.codEmpresa = 1 order by codLote desc """
        else:
            sql = """ SELECT 
                        codLote, 
                        descricao as nomeLote 
                    FROM tcl.Lote  l
                    WHERE 
                    l.descricao like '%PREV%' and l.codEmpresa = 4 order by codLote desc """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return lotes
