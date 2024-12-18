import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco


class Lote():
    def __init__(self,codLote = None, codEmpresa = '1', codPlano = None):
        self.codLote = codLote
        self.codEmpresa = codEmpresa
        self.codPlano = codPlano


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


    def obterLotesCsw(self):
        """
        Get dos Lotes cadastrados no CSW para PREVISAO.
        """

        if self.codEmpresa == '1':
            sql = """ 
            SELECT 
                codLote, 
                descricao as nomeLote 
            FROM 
                tcl.Lote  l
            WHERE 
                l.descricao like '%PREV%' 
                and l.codEmpresa = 1 
                order by codLote desc 
                """
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

    def obterLotesporPlano(self):
        sql = """
        SELECT 
            plano, 
            lote, 
            nomelote, 
            p."descricaoPlano" 
        FROM 
            pcp."LoteporPlano" l
        INNER JOIN 
            pcp."Plano" p 
            ON p.codigo = l.plano
        WHERE 
            plano = %s"""

        conn = ConexaoPostgreWms.conexaoEngine()
        try:
            df = pd.read_sql(sql, conn, params=(self.codPlano,))
            df['nomelote'] = df.apply(
                lambda r: self.transformarDataLote(r['lote'][2] if len(r['lote']) > 2 else '', r['nomelote'], r['lote']),
                axis=1)
        finally:
            conn.dispose()

        return df

    def transformarDataLote(self, mes, nomeLote, lote):
        '''Metodo utilizado para converter a data no formato ano-mes-dia'''
        if mes == 'R':
            mes1 = '/01/'
        elif mes == 'F':
            mes1 = '/02/'
        elif mes == 'M':
            mes1 = '/03/'
        elif mes == 'A':
            mes1 = '/04/'
        elif mes == 'I':
            mes1 = '/05/'
        elif mes == 'L':
            mes1 = '/06/'
        elif mes == 'L':
            mes1 = '/07/'
        elif mes == 'G':
            mes1 = '/08/'
        elif mes == 'S':
            mes1 = '/09/'
        elif mes == 'O':
            mes1 = '/10/'
        elif mes == 'N':
            mes1 = '/10/'
        elif mes == 'D':
            mes1 = '/12/'
        else:
            mes1 = '//'
        return lote[3:5] + mes1 + '20' + lote[:2] + '-' + nomeLote

    def consultarLoteEspecificoCsw(self):
        sql = """Select codLote, descricao as nomeLote from tcl.lote where codEmpresa= """ + str(
            self.codEmpresa) + """ and codLote =""" + "'" + self.codLote + "'"

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        nomeLote = lotes['nomeLote'][0]
        nomeLote = nomeLote[:2] + '-' + nomeLote

        return nomeLote