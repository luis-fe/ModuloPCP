import gc

import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms

class Produto():
    def __init__(self, codSku = None):
        self.codSku = codSku

    def conversaoSKUparaSKUPartes(self):
        '''Metodo que atribui o codigo sku da Parte ao sku do Produto Acabado
            return:
            DataFrame [{
            redParte, # o codigo sku da Parte Semiacabada
            codItem #o codigo sku do produto pai
            }]
        '''

        sql = """
        SELECT cv.CodComponente as redParte, cv.codProduto, cv2.codSortimento, cv2.seqTamanho as codSeqTamanho, cv2.quantidade
        FROM tcp.ComponentesVariaveis cv
        inner join tcp.CompVarSorGraTam cv2 on cv2.codEmpresa = cv.codEmpresa and cv2.codProduto = cv.codProduto and cv.codSequencia = cv2.sequencia 
        WHERE cv.codEmpresa = 1 and cv.codClassifComponente in (10,12) and cv.codProduto like '%-0'
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                relacaoPartes = pd.DataFrame(rows, columns=colunas)

        relacaoPartes['codSeqTamanho'] = relacaoPartes['codSeqTamanho'].astype(str)
        relacaoPartes['codSortimento'] = relacaoPartes['codSortimento'].astype(str)


        sl2Itens2 = """
            select codigo as "codItem", "codSortimento"::varchar, "codSeqTamanho"::varchar, '0'||"codItemPai"||'-0' as "codProduto"  from "PCP".pcp.itens_csw ic 
            where ic."codItemPai" like '1%'
            """

        conn = ConexaoPostgreWms.conexaoEngine()
        itens = pd.read_sql(sl2Itens2, conn)

        relacaoPartes = pd.merge(relacaoPartes, itens, on=['codProduto', 'codSortimento', 'codSeqTamanho'])

        return relacaoPartes

    def RecarregarItens(self):
        # passo 1: obter ultimo registro do sql itens_csw
        sql = """
        select max(codigo::int) as maximo from "PCP".pcp.itens_csw
        """

        conn = ConexaoPostgreWms.conexaoEngine()

        sqlMax = pd.read_sql(sql, conn)
        maximo = sqlMax['maximo'][0]

        sqlCSWItens = """
        SELECT 
            i.codigo , 
            i.nome , 
            i.unidadeMedida, 
            i2.codItemPai, 
            i2.codSortimento , 
            i2.codSeqTamanho,
            i2.codCor   
        FROM 
            cgi.Item i
        JOIN 
            Cgi.Item2 i2 on i2.coditem = i.codigo and i2.Empresa = 1
        WHERE 
            i.unidadeMedida in ('PC','KIT') 
            and (i2.codItemPai like '1%' or i2.codItemPai like '2%'or i2.codItemPai like '3%'or i2.codItemPai like '5%'or i2.codItemPai like '6%' )
            and i2.codItemPai > 0 and i.codigo > """ + str(maximo)

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlCSWItens)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()
        consulta['categoria'] = '-'

        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('CAMISA', row['nome'], 'CAMISA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('POLO', row['nome'], 'POLO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BATA', row['nome'], 'CAMISA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('TRICOT', row['nome'], 'TRICOT', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BONE', row['nome'], 'BONE', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('CARTEIRA', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('TSHIRT', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('REGATA', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BLUSAO', row['nome'], 'AGASALHOS', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BABY', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('JAQUETA', row['nome'], 'JAQUETA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('CARTEIRA', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BONE', row['nome'], 'BONE', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('CINTO', row['nome'], 'CINTO', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('PORTA CAR', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('CUECA', row['nome'], 'CUECA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('MEIA', row['nome'], 'MEIA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('SUNGA', row['nome'], 'SUNGA', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('SHORT', row['nome'], 'SHORT', row['categoria']), axis=1)
        consulta['categoria'] = consulta.apply(
            lambda row: self.Categoria('BERMUDA', row['nome'], 'BERMUDA M', row['categoria']), axis=1)

        try:
            # Implantando no banco de dados do Pcp
            ConexaoPostgreWms.Funcao_InserirOFF(consulta, consulta['codigo'].size, 'itens_csw', 'append')
        except:
            print('segue o baile ')
        return consulta

    def Categoria(self,contem, valorReferencia, valorNovo, categoria):
        if contem in valorReferencia:
            return valorNovo
        else:
            return categoria


    def consultaMarcasDisponiveis(self):
        '''Metodo que consulta as marcas disponiveis '''

        sql = """
        select * from "PCP".pcp."Marcas"
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def categoriasDisponiveis(self):
        '''Metodo que consulta as marcas disponiveis '''

        sql = """
        select * from "PCP".pcp."Categorias"
        order by "nomeCategoria" asc
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta


    def consultaItensReduzidos(self):
        '''Metodo utilizado para consultar os itens reduzidos x informacoes: codPai , codCor , nome, categoria'''

        sql = """
        	select
                codigo,
                nome,
                "codItemPai",
                "codCor",
                categoria,
                "codSeqTamanho"
            from
                "PCP".pcp.itens_csw ic
            where
                "codItemPai" like '1%'
                or "codItemPai" like '2%'
                or "codItemPai" like '3%' 
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)
        #consulta['codigo'] = consulta['codigo'].astype(str).str.replace('.0','')
        return consulta

    def statusAFV(self):
        '''Metodo que consulta o status AFV dos skus '''

        sql = """
        SELECT
            b.Reduzido as codReduzido,
            'Bloqueado' as statusAFV
        FROM
            Asgo_Afv.EngenhariasBloqueadas b
        WHERE
            b.Empresa = 1
        union	
        SELECT
            b.Reduzido as codReduzido ,
            'Acompanhamento' as statusAFV
        FROM
            Asgo_Afv.EngenhariasAcompanhamento b
        WHERE
            b.Empresa = 1
        """

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


    def estoqueNat5(self):
        '''metodo que consulta o estoque da natureza 05 '''

        sql = """
    SELECT
        d.codItem as codReduzido,
        d.estoqueAtual
    FROM
        est.DadosEstoque d
    WHERE
        d.codEmpresa = 1
        and d.codNatureza = 5
        and d.estoqueAtual > 0
        """

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

    def emProducao(self):
        '''metodo que lista os produtos e as quantidades em processo de producao'''

        sql = """
        select
            ic.codigo as "codReduzido",
            sum(total_pcs) as "emProcesso"
        from
            "PCP".pcp.itens_csw ic
        inner join 
            "PCP".pcp.ordemprod o 
            on substring(o."codProduto",2,8) = "codItemPai"
            and o."codSortimento" = ic."codSortimento"::varchar
            and o."seqTamanho" = "codSeqTamanho"::varchar
        group by 
            ic.codigo 
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta


    def get_tamanhos(self):
        '''Metodo que retorna os tamanhos do tcp do csw '''

        sql = """
        	SELECT
                t.sequencia as codSeqTamanho, t.descricao as tam
            FROM
                tcp.Tamanhos t
            WHERE
                t.codEmpresa = 1 
        """

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




