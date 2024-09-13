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



