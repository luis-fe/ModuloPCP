import gc

import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
import pytz
from datetime import datetime

class LeadTimeCalculator:
    """
    Classe para calcular o Lead Time das fases de produção.

    Atributos:
        data_inicio (str): Data de início do intervalo para análise.
        data_final (str): Data final do intervalo para análise.
    """

    def __init__(self, data_inicio, data_final,tipoOPs = None, categorias = None, congelado=False):
        """
        Inicializa a classe com o intervalo de datas para análise.

        Args:
            data_inicio (str): Data de início do intervalo.
            data_final (str): Data final do intervalo.
            tipoOPs ([str]): tipos de Ops a serem filtradas
            categorias ([str]): tipos de categorias a serem filtradas
        """
        self.data_inicio = data_inicio
        self.data_final = data_final
        self.tipoOps = tipoOPs
        self.categorias = categorias
        self.congelado = congelado


    def obter_lead_time_fases(self):
        """
        Calcula o Lead Time para as fases de produção no intervalo especificado.

        Returns:
            pd.DataFrame: DataFrame contendo as informações de Lead Time por fase.
        """
        # Consulta SQL para obter os dados de saída
        if self.tipoOps != [] :
            result = [int(item.split('-')[0]) for item in self.tipoOps]
            result = f"({', '.join(str(x) for x in result)})"
            sql = """
            SELECT
                rf.numeroop,
                rf.codfase,
                rf2."metaLeadTime"::varchar,
                rf."seqRoteiro",
                rf."dataBaixa"||' '||rf."horaMov" as "dataBaixa",
                rf."totPecasOPBaixadas" as "Realizado"
            FROM
                "PCP".pcp.realizado_fase rf 
            join 
            "PCP".pcp."responsabilidadeFase" rf2 on rf2."codFase" = rf.codfase::varchar
            WHERE
                rf."dataBaixa"::date >= %s AND rf."dataBaixa"::date <= %s and codtipoop in """+result

        else:
            sql = """
            SELECT
                rf.numeroop,
                rf.codfase,
                rf2."metaLeadTime"::varchar,
                rf."seqRoteiro",
                rf."dataBaixa"||' '||rf."horaMov" as "dataBaixa",
                rf."totPecasOPBaixadas" as "Realizado"
            FROM
                "PCP".pcp.realizado_fase rf 
            join 
            "PCP".pcp."responsabilidadeFase" rf2 on rf2."codFase" = rf.codfase::varchar
            WHERE
                rf."dataBaixa"::date >= %s AND rf."dataBaixa"::date <= %s ;
            """

        sqlFasesCsw = """
                select
            f.nome as nomeFase,
            f.codFase as codfase
        FROM
            tcp.FasesProducao f
        WHERE
            f.codempresa = 1
            and f.codFase >400
            and f.codFase <500
        """
        # Consulta SQL para obter os dados de entrada NO CSW (maior velocidade de processamento))
        sql_entrada = """
                    SELECT
                        o.numeroop as numeroop,
                        (
                        select
                            e.descricao
                        from
                            tco.OrdemProd op
                        join tcp.Engenharia e on
                            e.codengenharia = op.codproduto
                            and e.codempresa = 1
                        WHERE
                            op.codempresa = 1
                            and op.numeroop = o.numeroOP) as nome,
                        o.dataBaixa,
                        o.seqRoteiro,
                        o.horaMov as horaMovEntrada, (select
                            op.codTipoOP 
                        from
                            tco.OrdemProd op
                        WHERE
                            op.codempresa = 1
                            and op.numeroop = o.numeroOP) as codtipoop
                    FROM
                        tco.MovimentacaoOPFase o
                    WHERE
                        o.codEmpresa = 1
                        AND O.databaixa >= DATEADD(DAY,
                        -30,
                        GETDATE())
                            """

        # Conectar ao banco de dados
        conn = ConexaoPostgreWms.conexaoEngine()

        # Executar as consultas
        saida = pd.read_sql(sql, conn, params=(self.data_inicio, self.data_final))

        with ConexaoBanco.Conexao2() as connCSW:
                with connCSW.cursor() as cursor:
                    cursor.execute(sql_entrada)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    entrada = pd.DataFrame(rows, columns=colunas)

                    cursor.execute(sqlFasesCsw)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    sqlFasesCsw = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()


        # Processar os dados
        entrada['seqRoteiro'] = entrada['seqRoteiro'] + 1
        entrada.rename(columns={'dataBaixa': 'dataEntrada'}, inplace=True)
        saida = pd.merge(saida, entrada, on=['numeroop', 'seqRoteiro'])
        saida = saida.drop_duplicates()
        saida = pd.merge(saida,sqlFasesCsw,on='codfase')

        # Verifica e converte para datetime se necessário



        saida['dataEntrada'] = pd.to_datetime((saida['dataEntrada'] + ' ' + saida['horaMovEntrada']),errors='coerce')
        saida['dataBaixa'] = pd.to_datetime(saida['dataBaixa'] ,errors='coerce')



        saida['LeadTime(diasCorridos)'] = (saida['dataBaixa'] - saida['dataEntrada']).dt.total_seconds() / 3600
        print(saida['LeadTime(diasCorridos)'])
        saida['LeadTime(diasCorridos)'] =  saida['LeadTime(diasCorridos)'] / 24

        saida['RealizadoFase'] = saida.groupby('codfase')['Realizado'].transform('sum')
        saida['LeadTime(PonderadoPorQtd)'] = (saida['Realizado'] / saida['RealizadoFase']) * 100

        saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(diasCorridos)']*saida['LeadTime(PonderadoPorQtd)']
        saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(PonderadoPorQtd)'].round()
        saida['categoria'] = saida['nome'].apply(self.mapear_categoria)

        '''Inserindo as informacoes no banco para acesso temporario'''

        TotaltipoOp = [int(item.split('-')[0]) for item in self.tipoOps]
        id = self.data_inicio+'||'+self.data_final+'||'+str(TotaltipoOp)
        saida['id'] = id
        saida['diaAtual'] = self.obterdiaAtual()
        self.deletar_backup(id,"leadTimeFases")

        ConexaoPostgreWms.Funcao_InserirBackup(saida,saida['codfase'].size,'leadTimeFases','append')

        return saida


    def deletar_backup(self, id, tabela_temporaria):
        tabela_temporaria = '"'+tabela_temporaria+'"'
        delete = """
        DELETE FROM backup.%s
        WHERE id = %s
        """ % (tabela_temporaria, '%s')  # Substituindo tabela_temporaria corretamente

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(delete, (id,))
                conn.commit()

    def getLeadTimeFases(self):
        if self.congelado ==True:

            TotaltipoOp = [int(item.split('-')[0]) for item in self.tipoOps]
            id = self.data_inicio + '||' + self.data_final + '||' + str(TotaltipoOp)

            # Usando a string formatada na consulta SQL
            sql = f"""
                select
                    *
                from
                    backup."leadTimeFases" l
                where
                    l.id = %s
            """

            conn = ConexaoPostgreWms.conexaoEngine()
            saida = pd.read_sql(sql, conn, params=(id,))

            if self.categorias != []:
                categorias = pd.DataFrame(self.categorias, columns=["categoria"])
                saida = pd.merge(saida, categorias, on=['categoria'])

            if self.tipoOps != []:
                result = [int(item.split('-')[0]) for item in self.tipoOps]
                codtipoops = pd.DataFrame(result, columns=["codtipoop"])
                saida = pd.merge(saida, codtipoops, on=['codtipoop'])


        else:
            saida = self.obter_lead_time_fases()

            if self.categorias != []:
                categorias = pd.DataFrame(self.categorias, columns=["categoria"])
                saida = pd.merge(saida, categorias, on=['categoria'])

            if self.tipoOps != []:
                result = [int(item.split('-')[0]) for item in self.tipoOps]
                codtipoops = pd.DataFrame(result, columns=["codtipoop"])
                saida = pd.merge(saida, codtipoops, on=['codtipoop'])

        saida = saida.groupby(["codfase"]).agg({"LeadTime(diasCorridos)": "mean", "Realizado": "sum",
                                                    "LeadTime(PonderadoPorQtd)": 'sum', 'nomeFase': 'first','metaLeadTime':'first'}).reset_index()

        saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(PonderadoPorQtd)'] / 100
        saida['LeadTime(diasCorridos)'] = saida['LeadTime(diasCorridos)'].round()
        saida.fillna('-',inplace=True)
        return saida

    def mapear_categoria(self,nome):
        categorias_map = {
            'CAMISA': 'CAMISA',
            'POLO': 'POLO',
            'BATA': 'CAMISA',
            'TRICOT': 'TRICOT',
            'BONE': 'BONE',
            'CARTEIRA': 'CARTEIRA',
            'TSHIRT': 'CAMISETA',
            'REGATA': 'CAMISETA',
            'BLUSAO': 'AGASALHOS',
            'BABY': 'CAMISETA',
            'JAQUETA': 'JAQUETA',
            'CINTO': 'CINTO',
            'PORTA CAR': 'CARTEIRA',
            'CUECA': 'CUECA',
            'MEIA': 'MEIA',
            'SUNGA': 'SUNGA',
            'SHORT': 'SHORT',
            'BERMUDA': 'BERMUDA'
        }
        for chave, valor in categorias_map.items():
            if chave in nome.upper():
                return valor
        return '-'

    def getLeadTimeFaccionistas(self, faccionistas):

        sql = """
        SELECT
        	r.codFase ,
        	r.codFaccio as codfaccionista ,
        	r.codOP ,
        	r.dataEmissao as dataEntrada, op.codProduto , e.descricao as nome
        FROM
        	tct.RetSimbolicoNF r
        inner join 
        	tco.OrdemProd op on op.codEmpresa = 1 and op.numeroOP = r.codOP 
        inner JOIN 
        	tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
        WHERE
        	r.Empresa = 1 and r.codFase in (429, 431, 455, 459) and r.dataEmissao >= DATEADD(DAY,
                        -80,
                        GETDATE()) and r.dataEmissao <=  '""" + self.data_final + """'"""

        sqlRetornoFaccionista = """
        SELECT
            r.codFase  ,
            r.codFaccio as codfaccionista,
            r.codOP ,
            r.quantidade as Realizado ,
            r.dataEntrada as dataBaixa,
            op.codtipoop as codtipoop
        FROM
            tct.RetSimbolicoNFERetorno r
        inner join 
            tco.OrdemProd op on op.codEmpresa = 1 and op.numeroOP = r.codOP 
        inner JOIN 
            tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
        WHERE
            r.Empresa = 1 and r.codFase in (429, 431, 455, 459) and r.dataEntrada >= '"""+self.data_inicio +"""'and r.dataEntrada <=  '"""+self.data_final +"""'"""

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                realizado = pd.DataFrame(rows, columns=colunas)

                cursor.execute(sqlRetornoFaccionista)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlRetornoFaccionista = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        realizado['categoria'] = '-'
        realizado['nome'] = realizado['nome'].astype(str)
        faccionistas['codfaccionista'] = faccionistas['codfaccionista'].astype(str)
        realizado['codfaccionista'] = realizado['codfaccionista'].astype(str)
        sqlRetornoFaccionista['codfaccionista'] = sqlRetornoFaccionista['codfaccionista'].astype(str)

        realizado['categoria'] = realizado['nome'].apply(self.mapear_categoria)
        faccionistas = faccionistas.drop(columns='categoria')

        realizado = pd.merge(realizado,faccionistas,on='codfaccionista',how='left')
        realizado = pd.merge(realizado,sqlRetornoFaccionista,on=['codfaccionista','codFase','codOP'])

        realizado.fillna('-',inplace=True)
        # Verifica e converte para datetime se necessário
        realizado['dataEntrada'] = pd.to_datetime(realizado['dataEntrada'], errors='coerce')
        realizado['dataBaixa'] = pd.to_datetime(realizado['dataBaixa'], errors='coerce')
        realizado['LeadTime(diasCorridos)'] = (realizado['dataBaixa'] - realizado['dataEntrada']).dt.days

        # Convertendo a lista em um DataFrame

        if self.tipoOps != []:
            result = [int(item.split('-')[0]) for item in self.tipoOps]
            codtipoops = pd.DataFrame(result, columns=["codtipoop"])

            realizado = pd.merge(realizado, codtipoops, on=['codtipoop'])

        if self.categorias != []:
            categoriasData = pd.DataFrame(self.categorias, columns=["categoria"])
            realizado = pd.merge(realizado, categoriasData, on='categoria')

        realizado['Realizadofac'] = realizado.groupby('codfaccionista')['Realizado'].transform('sum')
        realizado['LeadTime(PonderadoPorQtd)'] = (realizado['Realizado'] / realizado['Realizadofac']) * 100

        realizado['LeadTime(PonderadoPorQtd)'] = realizado['LeadTime(diasCorridos)'] * realizado['LeadTime(PonderadoPorQtd)']
        realizado['LeadTime(PonderadoPorQtd)'] = realizado['LeadTime(PonderadoPorQtd)'].round()
        realizado = realizado.groupby(["codfaccionista"]).agg({"LeadTime(diasCorridos)": "mean", "Realizado": "sum",
                                                "LeadTime(PonderadoPorQtd)": 'sum', 'apelidofaccionista': 'first'}).reset_index()
        realizado['LeadTime(PonderadoPorQtd)'] = realizado['LeadTime(PonderadoPorQtd)'] / 100
        realizado['LeadTime(diasCorridos)'] = realizado['LeadTime(diasCorridos)'].round()
        print('realizado faccionistas:')
        print(realizado)
        return realizado
    def leadTimeCategoria(self):
        conn = ConexaoPostgreWms.conexaoEngine()

        if self.tipoOps != []:
            result = [int(item.split('-')[0]) for item in self.tipoOps]
            result = f"({', '.join(str(x) for x in result)})"

            sqlMovPCP = """
             SELECT
                o.numeroOP as OpPCP,
                o.dataBaixa as dataBaixaPCP,
                o.horaMov as horaMovPCP,
                o.totPecasOPBaixadas as RealizadoPCP
            FROM
                tco.MovimentacaoOPFase o
            inner Join
                tco.OrdemProd op on
                op.codempresa = o.codempresa
                and op.numeroop = o.numeroOP 
            WHERE
                op.codEmpresa = 1
                and dataBaixa <= '"""+self.data_final+"""' and o.codFase = 401 """

            sqlMovEntradaEstoque = """
                select rf."codEngenharia",
                rf.numeroop ,
                rf.codfase:: varchar as "codFase", rf."seqRoteiro" , rf."dataBaixa"::date ,  rf."horaMov"::time,
                rf."totPecasOPBaixadas" as "Realizado", rf."descOperMov" as operador, rf.chave 
            from
                "PCP".pcp.realizado_fase rf 
            where 
                rf."dataBaixa"::date >= %s 
                and rf."dataBaixa"::date <= %s and codFase in (236, 449) and codtipoop in """ + result

            MovEntradaEstoque = pd.read_sql(sqlMovEntradaEstoque, conn, params=(self.data_inicio, self.data_final))

            with ConexaoBanco.Conexao2() as connCsw:
                with connCsw.cursor() as cursor:
                    cursor.execute(sqlMovPCP)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    MovPCP = pd.DataFrame(rows, columns=colunas)


        else:

            sqlMovPCP = """
             SELECT
                o.numeroOP as OpPCP,
                o.dataBaixa as dataBaixaPCP,
                o.horaMov as horaMovPCP,
                o.totPecasOPBaixadas as RealizadoPCP
            FROM
                tco.MovimentacaoOPFase o
            inner Join
                tco.OrdemProd op on
                op.codempresa = o.codempresa
                and op.numeroop = o.numeroOP 
            WHERE
                op.codEmpresa = 1
                and dataBaixa <= '"""+self.data_final+"""' and o.codFase = 401 """

            sqlMovEntradaEstoque = """
                select rf."codEngenharia",
                rf.numeroop ,
                rf.codfase:: varchar as "codFase", rf."seqRoteiro" , rf."dataBaixa"::date ,  rf."horaMov"::time,
                rf."totPecasOPBaixadas" as "Realizado", rf."descOperMov" as operador, rf.chave 
            from
                "PCP".pcp.realizado_fase rf 
            where 
                rf."dataBaixa"::date >= %s 
                and rf."dataBaixa"::date <= %s and codFase in (236, 449) """
            MovEntradaEstoque = pd.read_sql(sqlMovEntradaEstoque, conn, params=(self.data_inicio, self.data_final))

            with ConexaoBanco.Conexao2() as connCsw:
                with connCsw.cursor() as cursor:
                    cursor.execute(sqlMovPCP)
                    colunas = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    MovPCP = pd.DataFrame(rows, columns=colunas)



        MovEntradaEstoque['OpPCP'] = np.where(
            MovEntradaEstoque['numeroop'].str.endswith('-001'),
            MovEntradaEstoque['numeroop'],
            MovEntradaEstoque['numeroop'].str.slice(stop=-4) + '-001'
        )

        leadTime = pd.merge(MovEntradaEstoque, MovPCP, on='OpPCP', how='left')

        # Verifica e converte para datetime se necessário
        leadTime['dataBaixaPCP'] = pd.to_datetime(leadTime['dataBaixaPCP'], errors='coerce')
        leadTime['horaMovPCP'] = pd.to_datetime(leadTime['horaMovPCP'], format='%H:%M:%S', errors='coerce').dt.time
        leadTime['dataBaixa'] = pd.to_datetime(leadTime['dataBaixa'], errors='coerce')
        leadTime['horaMov'] = pd.to_datetime(leadTime['horaMov'], format='%H:%M:%S', errors='coerce').dt.time
        leadTime['LeadTime(diasCorridos)'] = (leadTime['dataBaixa'] - leadTime['dataBaixaPCP']).dt.days

        # Converter para string usando o formato desejado
        leadTime['dataBaixaPCP'] = leadTime['dataBaixaPCP'].dt.strftime('%Y-%m-%d')
        leadTime['dataBaixa'] = leadTime['dataBaixa'].dt.strftime('%Y-%m-%d')
        leadTime['horaMovPCP'] = leadTime['horaMovPCP'].apply(
            lambda x: x.strftime('%H:%M:%S') if pd.notnull(x) else None)
        leadTime['horaMov'] = leadTime['horaMov'].apply(lambda x: x.strftime('%H:%M:%S') if pd.notnull(x) else None)

        sqlNomeEngenharia = """
        select ic."codItemPai"::varchar , max(ic.nome)::varchar as nome from "PCP".pcp.itens_csw ic where ("codItemPai" like '1%') or ("codItemPai" like '5%') or ("codItemPai" like '2%') group by "codItemPai"
        """
        NomeEngenharia = pd.read_sql(sqlNomeEngenharia, conn)

        NomeEngenharia['codEngenharia'] = np.where(
            NomeEngenharia['codItemPai'].str.startswith(('1', '2')),
            '0' + NomeEngenharia['codItemPai'] + '-0',
            NomeEngenharia['codItemPai'] + '-0'
        )
        leadTime = pd.merge(leadTime, NomeEngenharia, on='codEngenharia', how='left')

        leadTime['categoria'] = leadTime['nome'].apply(self.mapear_categoria)
        leadTime = leadTime.drop_duplicates()

        TotalPecas = leadTime['Realizado'].sum()
        leadTime['LeadTime(PonderadoPorQtd)'] = (leadTime['Realizado'] / TotalPecas) * leadTime[
            'LeadTime(diasCorridos)']
        leadTimePonderado = leadTime['LeadTime(PonderadoPorQtd)'].sum()

        leadTime['RealizadoCategoria'] = leadTime.groupby('categoria')['Realizado'].transform('sum')
        leadTime['LeadTimePonderado(categoria)'] = (leadTime['Realizado'] / leadTime['RealizadoCategoria']) * 100

        leadTime['LeadTimePonderado(diasCorridos)'] = leadTime['LeadTime(diasCorridos)'] * leadTime[
            'LeadTimePonderado(categoria)'].round(2)
        leadTime['LeadTimePonderado(diasCorridos)'] = leadTime['LeadTimePonderado(diasCorridos)'].round()

        try:
            leadTimeMedioGeral = leadTime['LeadTime(diasCorridos)'].mean().round()
        except:
            leadTimeMedioGeral = leadTime['LeadTime(diasCorridos)'].mean()


        leadTime_ = leadTime.groupby(["categoria"]).agg({"LeadTime(diasCorridos)": "mean", "Realizado": "sum",
                                                         "LeadTimePonderado(diasCorridos)": 'sum'}).reset_index()
        leadTime_ = leadTime_[leadTime_['categoria'] != '-']
        leadTime_['LeadTime(diasCorridos)'] = leadTime_['LeadTime(diasCorridos)'].round()
        leadTime_['LeadTimePonderado(diasCorridos)'] = leadTime_['LeadTimePonderado(diasCorridos)'] / 100
        leadTime_['LeadTimePonderado(diasCorridos)'] = leadTime_['LeadTimePonderado(diasCorridos)'].apply(np.ceil)

        MetaCategoria = self.ObterCategorias()
        leadTime_ = pd.merge(leadTime_, MetaCategoria, on='categoria', how='left')

        dados = {
            '01-leadTimeMedioGeral': f'{leadTimeMedioGeral} dias',
            '02-LeadTimeMediaPonderada': f'{round(leadTimePonderado)} dias',
            '03-TotalPeças': f'{TotalPecas} pçs',
            '04-LeadTimeCategorias': leadTime_.to_dict(orient='records')}

        return pd.DataFrame([dados])


    def obterdiaAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return pd.to_datetime(agora)

    def LimpezaBackpCongelamento(self,QuantidadeDiasEmBackup):
        QuantidadeDiasEmBackup = "'"+str(QuantidadeDiasEmBackup)+" days'"
        delete = """
        		delete 
                    from
                        backup."leadTimeFases" l
                    where 
                	    l."diaAtual"::Date  < CURRENT_DATE - INTERVAL """+QuantidadeDiasEmBackup

        with ConexaoPostgreWms.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(delete,)
                conn.commit()

    def ObterCategorias(self):
        sql = """Select "nomecategoria" as categoria, "leadTime" as meta from pcp.categoria """
        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql, conn)

        return consulta





