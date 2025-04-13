from connection import ConexaoPostgreWms,ConexaoBanco
import pandas as pd
from models import ProdutosClass

class FaseProducao():
    '''Clase para  as informacoes de Fase de Producao'''

    def __init__(self, codFase = None, responsavel = None, leadTimeMeta = None, nomeFase = None):
        '''Construtor da clase
        params:
        codFase - opcional,
        responsavel -opcional,
        lead time -opcional,
        nomeFase - opcional

        caso nao passe parametros assume-se que as informacoes sao para faccionistas (s);
        '''
        self.codFase = codFase
        self.responsavel = responsavel
        self.leadTimeMeta = leadTimeMeta

        # Encontrando o nome da fase no ERP CSW
        if nomeFase == None and codFase != None:
            sqlCsw = """
                    SELECT
                        f.codFase ,
                        f.nome as nomeFase
                    FROM
                        tcp.FasesProducao f
                    WHERE
                        f.codEmpresa = 1
                    and f.codFase = """+str(self.codFase)

            with ConexaoBanco.Conexao2() as connCsw:
                with connCsw.cursor() as cursor:
                    cursor.execute(sqlCsw)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    consulta = pd.DataFrame(rows, columns=colunas)
            if consulta.empty:
                self.nomeFase = '-'
            else:
                self.nomeFase = consulta['nomeFase'][0]
        else:
            self.nomeFase = nomeFase


    def InserirMetaLT_Responsavel(self):
        insert = """
        insert into pcp."responsabilidadeFase"  ("codFase" , responsavel, "metaLeadTime") values (%s , %s, %s) 
        """

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(insert,(str(self.codFase), self.responsavel, int(self.leadTimeMeta)))
                conn.commit()

        return pd.DataFrame([{'status':True, 'Mensagem':f'Responsavel e Lead Time inserirdos com sucesso na fase {self.codFase}-{self.nomeFase}'}])

    def ConsultarFases(self):
        consulta = """
        select "codFase" , responsavel, "metaLeadTime" from pcp."responsabilidadeFase" rf
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta,conn)
        return consulta

    def UpdateMetaLT_Responsavel(self):
        update = """UPDATE pcp."responsabilidadeFase" 
        SET responsavel = %s , "metaLeadTime"= %s
        where "codFase" = %s
        """
        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(update,(self.responsavel, self.leadTimeMeta,self.codFase,))
                conn.commit()

        return pd.DataFrame([{'status':True, 'Mensagem':f'Responsavel e Lead Time alterados com sucesso na fase {self.codFase}-{self.nomeFase}'}])

    def AlterarMetaLT_Responsave(self):

        pesquisa = self.ConsultarFases()
        pesquisa = pesquisa[pesquisa['codFase'] ==self.codFase]

        if pesquisa.empty:
            alteracao =self.InserirMetaLT_Responsavel()
            return alteracao
        else:
            alteracao = self.UpdateMetaLT_Responsavel()
            return alteracao

    def ConsultaFasesProdutivaCSWxGestao(self):

        sqlCsw = """
                SELECT
                    f.codFase ,
                    f.nome as nomeFase
                FROM
                    tcp.FasesProducao f
                WHERE
                    f.codEmpresa = 1
                and f.codFase > 400 and f.codFase < 599
                order by codFase asc
                """


        with ConexaoBanco.Conexao2() as connCsw:
            with connCsw.cursor() as cursor:
                cursor.execute(sqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        consultaFasesGestao = self.ConsultarFases()
        consulta['codFase'] = consulta['codFase'].astype(str)
        consulta = pd.merge(consulta,consultaFasesGestao,on='codFase',how='left')
        consulta.fillna('-',inplace=True)
        return consulta


    def cargaPCp(self):

        # 1 Consulta sql para obter as OPs em aberto no sistema do ´PCP
        sqlCarga = """
                    select 
                        codreduzido as "codItem", 
                        sum(total_pcs) as cargaPCP  
                    from 
                        pcp.ordemprod o 
                    where 
                        codreduzido is not null
                        and 
                        "codFaseAtual" = '401'
                    group by 
                        codreduzido
                """

        conn = ConexaoPostgreWms.conexaoEngine()

        sqlCarga = pd.read_sql(sqlCarga,conn)
        return sqlCarga



    def cargaPartes(self, relacaoPartes = None):
        '''Metodo para obter a carga de producao convertida em Partes - Semiacabado'''

        try:
            if relacaoPartes == 'None':
                Df_relacaoPartes = pd.DataFrame()
        except:
            Df_relacaoPartes = relacaoPartes




        # 1 Consulta sql para obter as OPs em aberto no sistema do ´PCP
        sqlCarga = """
            select 
                codreduzido as "codItem", 
                sum(total_pcs) as carga  
            from 
                pcp.ordemprod o 
            where 
                codreduzido is not null
                and 
                "codProduto" like '0%'
                and 
                "codFaseAtual" <> '401'
            group by 
                codreduzido
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        cargaPai = pd.read_sql(sqlCarga,conn)

    # 2 - Obtendo o DE-PARA DAS PARTES:
        partesPecas = Df_relacaoPartes


    #3 - Realizando o merge
        cargas = cargaPai.copy()  # Criar uma cópia do DataFrame original

        cargaPartes = pd.merge(cargaPai,partesPecas , on='codItem')
        cargaPartes.drop(['codProduto','codSeqTamanho','codSortimento'], axis=1, inplace=True)


    # Drop do codProduto
        cargaPartes.drop('codItem', axis=1, inplace=True)

    # Rename do redParte para codProduto
        cargaPartes.rename(columns={'redParte': 'codItem'}, inplace=True)

    #concatenando
        cargas = pd.concat([cargas, cargaPartes], ignore_index=True)

        return cargas


    def pesquisandoRoteiroDuplicados(self):
        sql = '''
                select
            *
        from
            "PCP".pcp."Eng_Roteiro" er
        where
            substring("codEngenharia",5,5) 
                in (
                    select
                        substring("codEngenharia",5,5)
                    from
                        "PCP".pcp."Eng_Roteiro" er
                    where
                        er."codFase" = '415'
                    group by
                        substring("codEngenharia",5,5)
                    having
                        count(substring("codEngenharia", 5, 5))>1
                    )
            and er."codFase" = '415'
        '''



