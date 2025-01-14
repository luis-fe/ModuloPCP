import gc
import numpy as np
import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
import fastparquet as fp
from dotenv import load_dotenv, dotenv_values
import os

from models import PlanoClass, ProdutosClass, TendenciasPlano


class AnaliseMateriais():
    '''Classe criada para a analise das necessidades de materia prima, utilizada mo PCP'''


    def __init__(self, codPlano = None , codLote= None, consideraBloqueado = 'nao', codComponente = ''):

        self.codLote = codLote
        self.codPlano = codPlano
        self.consideraBloqueado = consideraBloqueado
        self.codComponente = codComponente


    def carregandoComponentes(self):
        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        # 1.2 - Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile(f'{caminhoAbsoluto}/dados/compVar.parquet')


        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()

        return df_loaded

    def estruturaItens(self, pesquisaPor = 'lote', arraySimulaAbc = 'nao'):

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
            sqlMetas = ''

            with ConexaoBanco.Conexao2() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sqlcsw)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    consumo = pd.DataFrame(rows, columns=colunas)


        else:

            inPesquisa = self.estruturaPrevisao()
            if arraySimulaAbc == 'nao':
                sqlMetas = TendenciasPlano.TendenciaPlano(self.codPlano, self.consideraBloqueado).tendenciaVendas('nao')
            else:
                sqlMetas = TendenciasPlano.TendenciaPlano(self.codPlano, self.consideraBloqueado).simulacaoProgramacao(arraySimulaAbc)


            consumo = self.carregandoComponentes()
            consumo = pd.merge(consumo,inPesquisa, on='codEngenharia')



        # Obtendo os consumos de todos os componentes relacionados nas engenharias


        sqlEstoque = """
                    SELECT
        	            d.codItem as CodComponente ,
        	            d.estoqueAtual
                    FROM
        	            est.DadosEstoque d
                    WHERE
        	            d.codEmpresa = 1
        	            and d.codNatureza in (1, 3, 2,10)
        	            and d.estoqueAtual > 0
                """

        sqlRequisicaoAberto = """
                 SELECT
                 	r.numOPConfec as OP,
        	        ri.codMaterial as CodComponente ,
        	        ri.nomeMaterial,
        	        ri.qtdeRequisitada as EmRequisicao
                FROM
        	        tcq.RequisicaoItem ri
                join 
                    tcq.Requisicao r on
        	        r.codEmpresa = 1
        	        and r.numero = ri.codRequisicao
                where
        	        ri.codEmpresa = 1
        	        and r.sitBaixa <0
                """

        sqlAtendidoParcial = """
                SELECT
        		    i.codPedido as numero,
        		    i.codPedidoItem as seqitem,
        		    i.quantidade as qtAtendida
        	    FROM
        		    Est.NotaFiscalEntradaItens i
        	    WHERE
        		    i.codempresa = 1 
        		    and i.codPedido >0 
        		    and codPedido in (select codpedido FROM sup.PedidoCompraItem p WHERE
        	        p.situacao in (0, 2))
                """

        sqlPedidos = """
		SELECT
			s.codigo as numero,
			'solicitacao' as tipo,
			i2.codItem as CodComponente,
			i.nome,
			s.quantidade  as qtdPedida,
			'-' as dataPrevisao,
				s.situacao as sitSugestao,
				itemSolicitacao as seqitem,
				'1' as fatCon
		FROM
			sup.SolicitacaoComprasItem s
		inner join Cgi.Item2 i2 on i2.Empresa = 1 and i2.codEditado = s.codItemEdt 
		inner join cgi.item i on i.codigo = i2.coditem
		WHERE
			s.codEmpresa = 1
			and s.situacao in (0, 2)
	union
        SELECT
	        p.codPedido as numero,
	        'pedido' as tipo,
	        p.codProduto as CodComponente,
	        i.nome,
	        p.quantidade as qtdPedida,
	        p.dataPrevisao,
	        p.situacao,
	        p.itemPedido as  seqitem,
	        p.fatCon
        from 
	        sup.PedidoCompraItem p
	    inner join cgi.item i on i.codigo = p.codProduto
        WHERE
	        p.situacao in (0, 2)
	        and p.codEmpresa = 1
                """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:

                cursor.execute(sqlEstoque)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlEstoque = pd.DataFrame(rows, columns=colunas)

                # Agrupando as requisicoes compromedito pelo CodComponente
                sqlEstoque = sqlEstoque.groupby(["CodComponente"]).agg(
                    {"estoqueAtual": "sum"}).reset_index()

                # Carregando as requisicoes em aberto
                cursor.execute(sqlRequisicaoAberto)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlRequisicaoAberto = pd.DataFrame(rows, columns=colunas)

                # Congelando o dataFrame de Requisicoes em aberto
                load_dotenv('db.env')
                caminhoAbsoluto = os.getenv('CAMINHO')
                sqlRequisicaoAberto.to_csv(f'{caminhoAbsoluto}/dados/requisicoesEmAberto.csv')

                # Agrupando as requisicoes compromedito pelo CodComponente
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

                sqlPedidos = pd.merge(sqlPedidos,sqlAtendidoParcial,on=['numero','seqitem'],how='left')

                sqlPedidos['qtAtendida'].fillna(0,inplace=True)
                sqlPedidos['SaldoPedCompras'] = sqlPedidos['qtdPedida'] - sqlPedidos['qtAtendida']

                # Congelando o dataFrame de Pedidos em aberto
                sqlPedidos['fatCon'] = sqlPedidos['fatCon'].str.replace("*;", "", regex=False).astype(int)

                load_dotenv('db.env')
                caminhoAbsoluto = os.getenv('CAMINHO')
                sqlPedidos.to_csv(f'{caminhoAbsoluto}/dados/pedidosEmAberto.csv')


                sqlPedidos = sqlPedidos.groupby(["CodComponente"]).agg(
                    {"SaldoPedCompras": "sum"}).reset_index()

                # Libera memória manualmente
                del rows
                gc.collect()

        sqlMetas['codSortimento'] = sqlMetas['codSortimento'].astype(str)
        sqlMetas['codSortimento'] = sqlMetas['codSortimento'].str.replace('.0','')

        sqlMetas['codSeqTamanho'] = sqlMetas['codSeqTamanho'].astype(str)

        Necessidade = pd.merge(sqlMetas, consumo, on=["codItemPai" , "codSeqTamanho" , "codSortimento"], how='left')

        # Salvar o DataFrame na memoria:
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        Necessidade.to_csv(f'{caminhoAbsoluto}/dados/NecessidadePrevisao{self.codPlano}.csv')

        Necessidade['faltaProg (Tendencia)'] = Necessidade['faltaProg (Tendencia)'] * Necessidade['quantidade']

        Necessidade['disponivelVendas'] = Necessidade['disponivel'] * Necessidade['quantidade']




        Necessidade = Necessidade.groupby(["CodComponente"]).agg(
            {"disponivelVendas": "sum",
             "faltaProg (Tendencia)": "sum",
             "descricaoComponente":'first',
             "unid":'first'
             }).reset_index()
        Necessidade = pd.merge(Necessidade, sqlPedidos,on='CodComponente',how='left')
        Necessidade = pd.merge(Necessidade, sqlRequisicaoAberto,on='CodComponente',how='left')
        Necessidade = pd.merge(Necessidade, sqlEstoque,on='CodComponente',how='left')

        Necessidade['SaldoPedCompras'].fillna(0,inplace=True)
        Necessidade['EmRequisicao'].fillna(0,inplace=True)
        Necessidade['estoqueAtual'].fillna(0,inplace=True)
        Necessidade['Necessidade faltaProg (Tendencia)'] = (Necessidade['faltaProg (Tendencia)'])+Necessidade['estoqueAtual']+Necessidade['SaldoPedCompras']-Necessidade['EmRequisicao']
                                            # -0 + 1.747 + 2 -741,49 ( o negativo significa necessidade de compra)
        Necessidade['Necessidade faltaProg (Tendencia)'] = Necessidade['Necessidade faltaProg (Tendencia)'].where(Necessidade['Necessidade faltaProg (Tendencia)'] < 0, 0)

        Necessidade['estoqueAtual'] = Necessidade['estoqueAtual'].apply(self.formatar_float)
        Necessidade['EmRequisicao'] = Necessidade['EmRequisicao'].apply(self.formatar_float)
        Necessidade['SaldoPedCompras'] = Necessidade['SaldoPedCompras'].apply(self.formatar_float)

        informacoes = self.informacoesComponente()
        Necessidade = pd.merge(Necessidade, informacoes, on='CodComponente',how='left')
        Necessidade['loteMut'].fillna(1,inplace=True)
        Necessidade['LoteMin'].fillna(0,inplace=True)

        Necessidade['LeadTime'] = Necessidade['LeadTime'].apply(self.formatar_padraoInteiro)


        Necessidade.fillna('-',inplace=True)
        Necessidade.rename(
            columns={'CodComponente': '01-codReduzido',
                     'codEditado': '02-codCompleto',
                     'descricaoComponente': '03-descricaoComponente',
                     'fornencedorPreferencial': '04-fornencedorPreferencial',
                     'unid': '05-unidade',
                     'faltaProg (Tendencia)': '06-Necessidade faltaProg(Tendencia)',
                     'EmRequisicao': '07-EmRequisicao',
                     'estoqueAtual': '08-estoqueAtual',
                     'SaldoPedCompras': '09-SaldoPedCompras',
                     'Necessidade faltaProg (Tendencia)': '10-Necessidade Compra (Tendencia)',
                     'LeadTime': '13-LeadTime',
                     'LoteMin': '14-Lote Mínimo',
                     'loteMut': '11-Lote Mutiplo'
                     },
            inplace=True)

        # Função para ajustar a necessidade
        def ajustar_necessidade(necessidade, lote_multiplo, lotemin):
            necessidade = necessidade * -1
            if necessidade > 0 and necessidade < lotemin:

                return lotemin
            else:
                if lote_multiplo != 0:

                    return np.ceil(necessidade / lote_multiplo) * lote_multiplo
                else:
                    return necessidade

        # Aplicando o ajuste
        Necessidade["12-Necessidade Ajustada Compra (Tendencia)"] = Necessidade.apply(
            lambda row: ajustar_necessidade(row["10-Necessidade Compra (Tendencia)"], row["11-Lote Mutiplo"],row["14-Lote Mínimo"]), axis=1
        )

        Necessidade = Necessidade.drop(columns=['disponivelVendas'])
        Necessidade['12-Necessidade Ajustada Compra (Tendencia)'] = Necessidade['12-Necessidade Ajustada Compra (Tendencia)'].apply(self.formatar_float)
        Necessidade['10-Necessidade Compra (Tendencia)'] = Necessidade['10-Necessidade Compra (Tendencia)'] * -1
        Necessidade['11-Lote Mutiplo'] = Necessidade['11-Lote Mutiplo'].apply(self.formatar_float)
        Necessidade['10-Necessidade Compra (Tendencia)'] = Necessidade['10-Necessidade Compra (Tendencia)'].apply(self.formatar_float)
        Necessidade['14-Lote Mínimo'] = Necessidade['14-Lote Mínimo'].apply(self.formatar_float)
        Necessidade['06-Necessidade faltaProg(Tendencia)'] = Necessidade['06-Necessidade faltaProg(Tendencia)'].apply(self.formatar_float)
        Necessidade = Necessidade[Necessidade['02-codCompleto'] != '-']
        return Necessidade

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
        parquet_file = fp.ParquetFile(f'{caminhoAbsoluto}/dados/pedidos.parque'
                                      f't')

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
        df_loaded = df_loaded.drop_duplicates(subset='codItemPai')




        df_loaded['codEngenharia'] = '0'+df_loaded['codItemPai']+'-0'
        df_loaded['codItemPai'].fillna('-',inplace=True)


        #result = f"({', '.join(repr(x) for x in df_loaded['codItemPai'])})"


        return df_loaded



    def formatar_float(self,valor):
        try:
            return f'{valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível



    def informacoesComponente(self):
        '''Metodo de informacao dos componentes '''


        sql = """
        SELECT
            q.codigo as CodComponente ,
            f.nomeFornecedor as fornencedorPreferencial,
            q.diasEntrega as LeadTime,
            q.qtdMinCom as LoteMin,
            q.qtdMultCom as loteMut,
            q.fatorConversao
            ,(SELECT i2.codeditado from cgi.Item2 i2 WHERE i2.Empresa = 2 and i2.codcor> 0 and i2.coditem = q.codigo) as codEditado
        FROM
            Cgi.FornecHomologados f
        right join 
            Cgi.DadosQualidadeFornecedor q on
            q.codEmpresa = f.codEmpresa
            and f.codItem = q.codigo
            and f.codFornecedor = q.codFornecedor
        WHERE
            f.codEmpresa = 1
            and f.fornecedorPreferencial = 1
            and q.referenciaPrincipal = 1
            and q.codigo > 18
                """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consumo = pd.DataFrame(rows, columns=colunas)
        consumo['fatorConversao'].fillna(1,inplace=True)
        consumo['LeadTime'].fillna(1,inplace=True)
        consumo['CodComponente'] = consumo['CodComponente'].astype(str)
        #consumo['CodComponente'] = consumo['CodComponente'].str.replace('.0','')
        print(consumo['CodComponente'])

        return consumo


    def formatar_padraoInteiro(self,valor):
        try:
            return f'{valor:,.0f}'.replace(",", "X").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível

    def detalhaNecessidade(self):
        '''metodo que detalha a necessidade de um componente '''

        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')

        Necessidade = pd.read_csv(f'{caminhoAbsoluto}/dados/NecessidadePrevisao{self.codPlano}.csv')
        Necessidade['CodComponente'] = Necessidade['CodComponente'].astype(str)
        Necessidade['CodComponente'] = Necessidade['CodComponente'].str.replace('.0','')
        Necessidade = Necessidade[Necessidade['CodComponente']==self.codComponente].reset_index()
        Necessidade['Necessidade faltaProg (Tendencia)'] = Necessidade['faltaProg (Tendencia)'] * Necessidade['quantidade'] * -1


        Necessidade.rename(
            columns={'codEngenharia': '01-codEngenharia',
                     'codReduzido': '02-codReduzido',
                     'nome': '03-nome',
                     'tam': '04-tam',
                     'codCor': '05-codCor',
                     'qtdePedida': '06-qtdePedida',
                     'Ocorrencia em Pedidos':'07-Ocorrencia em Pedidos',
                     'statusAFV': '08-statusAFV',
                     'previcaoVendas': '09-previcaoVendas',
                     'faltaProg (Tendencia)': '10-faltaProg (Tendencia)',
                     'CodComponente': '11-CodComponente',
                     'unid': '12-unid',
                     'quantidade':'13-consumoUnit',
                     'Necessidade faltaProg (Tendencia)':'14-Necessidade faltaProg (Tendencia)'
                     },
            inplace=True)
        Necessidade = Necessidade.drop(columns=['Prev Sobra','Unnamed: 0','categoria','marca','index','descricaoComponente'
            ,'descricaoPlano','disponivel','dist%','emProcesso','estoqueAtual','codItemPai','codPlano','codSeqTamanho','codSortimento',
                                                'valorVendido','qtdeFaturada'])

        Necessidade['14-Necessidade faltaProg (Tendencia)'] = Necessidade['14-Necessidade faltaProg (Tendencia)'].round(2)

        return Necessidade

    def consultaImagem(self):
        sql = """
         SELECT ID
         FROM Utils_Persistence.Csw1Stream
         WHERE rotina = 'CCCGI015'
           AND documentoReferencia = 'Item-250101854'
           AND mimeType = 'image/jpeg'
         """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                row = cursor.fetchone()  # Recupera o ID do registro
                if row:
                    record_id = row[0]
                    stream_data = self.read_stream(record_id, conn)
                    return stream_data
                else:
                    return None

    def read_stream(self, record_id, conn):
        # Consulta para buscar o stream usando o ID
        sql = f"SELECT stream FROM Utils_Persistence.Csw1Stream WHERE ID = {record_id}"
        with conn.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()
            if row:
                stream_data = row[0]  # Objeto do tipo CacheInputStream
                print(f"Tipo do objeto: {type(stream_data)}")
                print(f"Stream: {stream_data}")

                bytes_data = b""
                buffer = bytearray(4096)  # Cria um buffer de 4 KB
                while True:
                    bytes_read = stream_data.read(buffer, 0, len(buffer))  # Lê no buffer
                    if bytes_read == -1:  # -1 indica EOF (fim do stream)
                        break
                    bytes_data += buffer[:bytes_read]  # Adiciona ao resultado
                return bytes_data
            else:
                return None

    def sqlEstoque(self):

        sql = """
                    SELECT
        	            d.codItem as CodComponente ,
        	            (select n.codnatureza||'-'||n.descricao from est.Natureza n WHERE n.codempresa = 1 and d.codNatureza = n.codnatureza)as natureza,
        	            i.nome,
        	            d.estoqueAtual
                    FROM
        	            est.DadosEstoque d
        	        join cgi.Item i on i.codigo = d.codItem 
                    WHERE
        	            d.codEmpresa = 1
        	            and d.codNatureza in (1, 3, 2,10)
        	            and d.estoqueAtual > 0
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consumo = pd.DataFrame(rows, columns=colunas)

        return consumo

    def sqlEstoqueComprometido(self):

        #1 - Caregando as requisicoes em aberto
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        consumo = pd.read_csv(f'{caminhoAbsoluto}/dados/requisicoesEmAberto.csv')

        consumo.drop(['Unnamed: 0'], axis=1, inplace=True)
        consumo['CodComponente'] = consumo['CodComponente'].astype(str)

        return consumo


    def sqlEstoquePedidosCompras(self):

        #1 - Caregando as requisicoes em aberto
        load_dotenv('db.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        consumo = pd.read_csv(f'{caminhoAbsoluto}/dados/pedidosEmAberto.csv')

        consumo.drop(['Unnamed: 0'], axis=1, inplace=True)
        consumo['CodComponente'] = consumo['CodComponente'].astype(str)

        return consumo