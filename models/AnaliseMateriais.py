import gc
import numpy as np
import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
import fastparquet as fp
from dotenv import load_dotenv, dotenv_values
import os

from models import PlanoClass, ProdutosClass


class AnaliseMateriais():
    '''Classe criada para a analise das necessidades de materia prima, utilizada mo PCP'''


    def __init__(self, codPlano = None , codLote= None):

        self.codLote = codLote
        self.codPlano = codPlano

    def estruturaItens(self, pesquisaPor = 'lote'):

        if pesquisaPor == 'lote':
            inPesquisa = """(select l.codengenharia from tcl.LoteSeqTamanho l WHERE l.empresa = 1 and l.codlote = '""" + self.codLote + """)"""
            sqlcsw = """
                        SELECT 
                            v.codProduto as codEngenharia, 
                            cv.codSortimento, 
                            cv.seqTamanho as codSeqTamanho,  
                            v.CodComponente,
                            (SELECT i.nome FROM cgi.Item i WHERE i.codigo = v.CodComponente) as descricaoComponente,
                            (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
                            cv.quantidade  
                        from 
                            tcp.ComponentesVariaveis v 
                        join 
                            tcp.CompVarSorGraTam cv 
                            on cv.codEmpresa = v.codEmpresa 
                            and cv.codProduto = v.codProduto 
                            and cv.sequencia = v.codSequencia 
                        WHERE 
                            v.codEmpresa = 1
                            and v.codProduto in """ + inPesquisa + """
                            and v.codClassifComponente <> 12
                    UNION 
                        SELECT 
                            v.codProduto as codEngenharia,  
                            l.codSortimento ,
                            l.codSeqTamanho as codSeqTamanho, 
                            v.CodComponente,
                            (SELECT i.nome FROM cgi.Item i WHERE  i.codigo = v.CodComponente) as descricaoComponente,
                            (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
                            v.quantidade  
                        from 
                            tcp.ComponentesPadroes  v 
                        join 
                            tcl.LoteSeqTamanho l 
                            on l.Empresa = v.codEmpresa 
                            and l.codEngenharia = v.codProduto 
                            and l.codlote = '""" + self.codLote + """'"""
        else:

            inPesquisa = self.estruturaPrevisao()
            print(inPesquisa)
            sqlcsw = """
                        SELECT 
                            v.codProduto as codEngenharia, 
                            cv.codSortimento, 
                            cv.seqTamanho as codSeqTamanho,  
                            v.CodComponente,
                            (SELECT i.nome FROM cgi.Item i WHERE i.codigo = v.CodComponente) as descricaoComponente,
                            (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
                            cv.quantidade  
                        from 
                            tcp.ComponentesVariaveis v 
                        join 
                            tcp.CompVarSorGraTam cv 
                            on cv.codEmpresa = v.codEmpresa 
                            and cv.codProduto = v.codProduto 
                            and cv.sequencia = v.codSequencia 
                        WHERE 
                            v.codEmpresa = 1
                            and v.codProduto in """ + inPesquisa + """
                            and v.codClassifComponente <> 12
                        UNION 
                        SELECT 
                            v.codProduto as codEngenharia,  
                            l.codSortimento ,
                            l.codSeqTamanho  as codSeqTamanho, 
                            v.CodComponente,
                            (SELECT i.nome FROM cgi.Item i WHERE  i.codigo = v.CodComponente) as descricaoComponente,
                            (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
                            v.quantidade  
                        from 
                            tcp.ComponentesPadroes  v 
                        join 
                            cgi.Item2 l
                            on l.Empresa  = v.codEmpresa
                            and v.codEmpresa = 1
                            and l.codCor  > 0
                            and '0'||l.codItemPai ||'-0' = v.codProduto
                        where
                            v.codProduto  in """ + inPesquisa



        # Obtendo os consumos de todos os componentes relacionados nas engenharias


        sqlEstoque = """
                    SELECT
        	            d.codItem as CodComponente ,
        	            d.estoqueAtual
                    FROM
        	            est.DadosEstoque d
                    WHERE
        	            d.codEmpresa = 1
        	            and d.codNatureza in (1, 3, 2)
        	            and d.estoqueAtual > 0
                """

        sqlRequisicaoAberto = """
                SELECT
        	        ri.codMaterial as CodComponente ,
        	        ri.qtdeRequisitada as EmRequisicao
                FROM
        	        tcq.RequisicaoItem ri
                join 
                    tcq.Requisicao r on
        	        r.codEmpresa = ri.codEmpresa
        	        and r.numero = ri.codRequisicao
                where
        	        ri.codEmpresa = 1
        	        and r.sitBaixa <0
                """

        sqlAtendidoParcial = """
                SELECT
        		    i.codPedido as pedCompra,
        		    i.codPedidoItem as seqitem,
        		    i.quantidade as qtAtendida
        	    FROM
        		    Est.NotaFiscalEntradaItens i
        	    WHERE
        		    i.codempresa = 1 
        		    and i.codPedido >0 
        		    and codPedido in (select codpedido FROM up.PedidoCompraItem p WHERE
        	        p.situacao in (0, 2))
                """

        sqlPedidos = """
                SELECT
        	        p.codPedido pedCompra,
        	        p.codProduto as CodComponente,
        	        p.quantidade as qtdPedida,
        	        p.dataPrevisao,
        	        p.itemPedido as seqitem,
        	        p.situacao
                from 
        	        sup.PedidoCompraItem p
                WHERE
        	        p.situacao in (0, 2)
        	        and p.codEmpresa = 1
                """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlcsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consumo = pd.DataFrame(rows, columns=colunas)

                cursor.execute(sqlEstoque)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlEstoque = pd.DataFrame(rows, columns=colunas)

                # Agrupando os componentes nas requisicoes em aberto
                cursor.execute(sqlRequisicaoAberto)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlRequisicaoAberto = pd.DataFrame(rows, columns=colunas)
                sqlRequisicaoAberto = sqlRequisicaoAberto.groupby(["CodComponente"]).agg(
                    {"EmRequisicao": "sum"}).reset_index()

                cursor.execute(sqlAtendidoParcial)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlAtendidoParcial = pd.DataFrame(rows, columns=colunas)

                cursor.execute(sqlPedidos)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlPedidos = pd.DataFrame(rows, columns=colunas)

                sqlPedidos = pd.merge(sqlPedidos,sqlAtendidoParcial,on=['pedCompra','seqitem'],how='left')

                sqlPedidos['qtAtendida'].fillna(0,inplace=True)
                sqlPedidos['SaldoPedCompras'] = sqlPedidos['qtdPedida'] - sqlPedidos['qtAtendida']
                sqlPedidos = sqlPedidos.groupby(["CodComponente"]).agg(
                    {"SaldoPedCompras": "sum"}).reset_index()

                # Libera memória manualmente
                del rows
                gc.collect()

        return consumo

    def metaLote(self):
        conn = ConexaoPostgreWms.conexaoEngine()
        # Obtendo as Previsao do Lote
        sqlMetas = """
        SELECT "codLote", "Empresa", "codEngenharia", "codSeqTamanho"::varchar, "codSortimento"::varchar, previsao
        FROM "PCP".pcp.lote_itens li
        WHERE "codLote" = %s
        """
        sqlMetas = pd.read_sql(sqlMetas,conn, params=(self.codLote,))

        consulta = """
               select 
                    codigo as "codItem", 
                    nome, 
                    "unidadeMedida" ,  
                    "codItemPai" , 
                    "codSortimento"::varchar as "codSortimento" , 
                    "codSeqTamanho"::varchar as "codSeqTamanho"  
               from 
                    pcp.itens_csw ic 
               """
        consulta = pd.read_sql(consulta, conn)

        # Verificar quais codItemPai começam com '1' ou '2'
        mask = consulta['codItemPai'].str.startswith(('1', '2'))
        # Aplicar as transformações usando a máscara
        consulta['codEngenharia'] = np.where(mask, '0' + consulta['codItemPai'] + '-0', consulta['codItemPai'] + '-0')

        sqlMetas = pd.merge(sqlMetas, consulta, on=["codEngenharia", "codSeqTamanho", "codSortimento"], how='left')
        sqlMetas['codItem'].fillna('-', inplace=True)

        return sqlMetas


    def estruturaPrevisao(self):

        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        # 1.2 - Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile(f'{caminhoAbsoluto}/dados/pedidos.parquet')

        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()
        plano = PlanoClass.Plano(self.codPlano)
        self.iniVendas, self.fimVendas = plano.pesquisarInicioFimVendas()
        self.iniFat, self.fimFat = plano.pesquisarInicioFimFat()
        produtos = ProdutosClass.Produto().consultaItensReduzidos()
        produtos.rename(
            columns={'codigo': 'codProduto'},
            inplace=True)
        df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)
        df_loaded['dataPrevFat'] = pd.to_datetime(df_loaded['dataPrevFat'], errors='coerce', infer_datetime_format=True)
        df_loaded['filtro'] = df_loaded['dataEmissao'] >= self.iniVendas
        df_loaded['filtro2'] = df_loaded['dataEmissao'] <= self.fimVendas
        df_loaded['filtro3'] = df_loaded['dataPrevFat'] >= self.iniFat
        df_loaded['filtro4'] = df_loaded['dataPrevFat'] <= self.fimFat
        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['filtro2'] == True].reset_index()
        # print(df_loaded['filtro3'].drop_duplicates())
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])
        df_loaded = df_loaded[df_loaded['filtro3'] == True].reset_index()
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])
        df_loaded = df_loaded[df_loaded['filtro4'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['situacaoPedido'] != '9']
        df_loaded['codProduto'] = df_loaded['codProduto'].astype(str)

        df_loaded = df_loaded.loc[:,
                         ['codProduto']]

        produtos['codItemPai'] = produtos['codItemPai'].astype(str)


        df_loaded = pd.merge(df_loaded, produtos, on='codProduto', how='left')

        df_loaded['codItemPai'].fillna('-', inplace=True)
        df_loaded = df_loaded.loc[:,
                         ['codItemPai']]
        df_loaded['codItemPai'] = df_loaded['codItemPai'].where(~df_loaded['codItemPai'].duplicated())

        print(df_loaded)



        df_loaded['codItemPai'] = '0'+df_loaded['codItemPai']+'-0'
        df_loaded['codItemPai'].fillna('-',inplace=True)

        #result = f"({', '.join(repr(x) for x in df_loaded['codItemPai'])})"

        #print(df_loaded)

        return df_loaded








