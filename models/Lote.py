import gc
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
from models import ProdutosClass


class Lote():
    '''Classe relativo ao controle de Lote de Producao que vem do ERP CSW'''
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

    def explodindoAsReferenciasLote(self, arrayCodLoteCsw):
        '''Metodo que explode (detalha) os skus previsionados no lote '''

        # 1 - Transformando o array em padrao "in sql"
        nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
        novo = ", ".join(nomes_com_aspas)
        #____________________________________________________________________

        # 2 - Pesquisando no ERP CSW o Lote vinculado
        sqlLotes = """
        select Empresa , t.codLote, codengenharia, t.codSeqTamanho , t.codSortimento , t.qtdePecasImplementadas as previsao FROM tcl.LoteSeqTamanho t
        WHERE t.Empresa = """ + self.codEmpresa + """and t.codLote in (""" + novo + """) 
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlLotes)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)
        del rows
        gc.collect()

        # 3- Implantando no banco de dados do Pcp, atualizando o cadastro de itens e atualizando o roteiro
        ConexaoPostgreWms.Funcao_InserirOFF(lotes, lotes['codLote'].size, 'lote_itens', 'append')

        #4 - Recarregando os itens
        ProdutosClass.Produto().RecarregarItens()
        self.carregarRoteiroEngLote(arrayCodLoteCsw)

        return lotes

    def carregarRoteiroEngLote(self, arrayCodLoteCsw):
        nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
        novo = ", ".join(nomes_com_aspas)

        sql = """
        SELECT p.codEngenharia , p.codFase , p.nomeFase, p.seqProcesso  FROM tcp.ProcessosEngenharia p
    WHERE p.codEmpresa = 1 and p.codEngenharia like '%-0' and 
    p.codEngenharia in (select l.codEngenharia from tcl.LoteEngenharia l WHERE l.empresa =""" + str(
            self.codEmpresa) + """ and l.codlote in ( """ + novo + """))"""

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                EngRoteiro = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        # Verificando as engenharias que ja existe:
        sqlPCP = """select distinct "codEngenharia", 'ok' as situacao from pcp."Eng_Roteiro" """

        conn2 = ConexaoPostgreWms.conexaoEngine()
        sqlPCP = pd.read_sql(sqlPCP, conn2)

        EngRoteiro = pd.merge(EngRoteiro, sqlPCP, on='codEngenharia', how='left')
        EngRoteiro.fillna('-', inplace=True)
        EngRoteiro = EngRoteiro[EngRoteiro['situacao'] == '-'].reset_index()
        EngRoteiro = EngRoteiro.drop(columns=['situacao', 'index'])
        print(EngRoteiro)
        if EngRoteiro['codEngenharia'].size > 0:
            # Implantando no banco de dados do Pcp
            ConexaoPostgreWms.Funcao_InserirOFF(EngRoteiro, EngRoteiro['codEngenharia'].size, 'Eng_Roteiro', 'append')
        else:
            print('segue o baile')

    def desvincularLotePlano(self):
        '''Metodo que desvincula um lote ao plano e ainda deleta a previsao dos itens no banco de dados PCP ."lote_itens" '''


        # Passo 1: Excluir o lote do plano vinculado
        deletarLote = """DELETE FROM pcp."LoteporPlano" WHERE lote = %s and plano = %s """
        conn = ConexaoPostgreWms.conexaoInsercao()
        cur = conn.cursor()
        cur.execute(deletarLote, (self.codLote, self.codPlano))
        conn.commit()

        # Passo 2: Verifica se o lote existe em outros planos
        conn2 = ConexaoPostgreWms.conexaoEngine()
        sql = """Select lote from pcp."LoteporPlano" WHERE lote = %s """
        verifca = pd.read_sql(sql, conn2, params=(self.codLote,))

        if verifca.empty:

            deletarLoteIntens = """Delete from pcp.lote_itens where "codLote" = %s  """
            cur.execute(deletarLoteIntens, (self.codLote,))
            conn.commit()

        else:
            print('sem lote para exlcuir dos lotes engenharias')
        cur.close()
        conn.close()
