import gc
import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco

class LeadTimeCalculator:
    """
    Classe para calcular o Lead Time das fases de produção.

    Atributos:
        data_inicio (str): Data de início do intervalo para análise.
        data_final (str): Data final do intervalo para análise.
    """

    def __init__(self, data_inicio, data_final,tipoOPs = None, categorias = None):
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
                rf."seqRoteiro",
                rf."dataBaixa",
                rf."totPecasOPBaixadas" as "Realizado"
            FROM
                "PCP".pcp.realizado_fase rf
            WHERE
                rf."dataBaixa"::date >= %s AND rf."dataBaixa"::date <= %s and codtipoop in """+result

        else:
            sql = """
            SELECT
                rf.numeroop,
                rf.codfase,
                rf."seqRoteiro",
                rf."dataBaixa",
                rf."totPecasOPBaixadas" as "Realizado"
            FROM
                "PCP".pcp.realizado_fase rf 
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
                        o.seqRoteiro
                    FROM
                        tco.MovimentacaoOPFase o
                    WHERE
                        o.codEmpresa = 1
                        AND O.databaixa >= DATEADD(DAY,
                        -30,
                        GETDATE())
                            """
        try:
            # Conectar ao banco de dados
            conn = ConexaoPostgreWms.conexaoEngine()

            # Executar as consultas
            saida = pd.read_sql(sql, conn, params=(self.data_inicio, self.data_final))
            with ConexaoBanco.Conexao2() as conn:
                with conn.cursor() as cursor:
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
            saida['dataEntrada'] = pd.to_datetime(saida['dataEntrada'], errors='coerce')
            saida['dataBaixa'] = pd.to_datetime(saida['dataBaixa'], errors='coerce')
            saida['LeadTime(diasCorridos)'] = (saida['dataBaixa'] - saida['dataEntrada']).dt.days

            saida['RealizadoFase'] = saida.groupby('codfase')['Realizado'].transform('sum')
            saida['LeadTime(PonderadoPorQtd)'] = (saida['Realizado'] / saida['RealizadoFase']) * 100

            saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(diasCorridos)']*saida['LeadTime(PonderadoPorQtd)']
            saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(PonderadoPorQtd)'].round()
            saida['categoria'] = saida['nome'].apply(self.mapear_categoria)

            '''Inserindo as informacoes no banco para acesso temporario'''
            TotaltipoOp = [int(item.split('-')[0]) for item in self.tipoOps]
            id = self.data_inicio+'||'+self.data_final+'||'+str(TotaltipoOp)
            saida['id'] = id
            self.deletar_backup(id,"leadTimeFases")
            ConexaoPostgreWms.Funcao_InserirBackup(saida,saida['codfase'].size,'leadTimeFases','append')

            return saida

        except Exception as e:
            print(f"Erro ao calcular o Lead Time: {e}")
            return None

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
        if self.categorias != []:
            chave = "'"
            # Transformando o array em string no formato desejado
            result = f"({', '.join([f'{chave}{item}{chave}' for item in self.categorias])})"

            TotaltipoOp = [int(item.split('-')[0]) for item in self.tipoOps]
            id = self.data_inicio + '||' + self.data_final + '||' + str(TotaltipoOp)

            # Usando a string formatada na consulta SQL
            sql = f"""
                select
                    *
                from
                    backup."leadTimeFases" l
                where
                    l.id = %s and l.categoria in {result}
            """

            conn = ConexaoPostgreWms.conexaoEngine()
            saida = pd.read_sql(sql, conn, params=(id,))

        else:
            saida = self.obter_lead_time_fases()


        saida = saida.groupby(["codfase"]).agg({"LeadTime(diasCorridos)": "mean", "Realizado": "sum",
                                                    "LeadTime(PonderadoPorQtd)": 'sum', 'nomeFase': 'first'}).reset_index()
        saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(PonderadoPorQtd)'] / 100
        saida['LeadTime(diasCorridos)'] = saida['LeadTime(diasCorridos)'].round()
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
        	r.dataEmissao, op.codProduto , e.descricao as nome
        FROM
        	tct.RetSimbolicoNF r
        inner join 
        	tco.OrdemProd op on op.codEmpresa = 1 and op.numeroOP = r.codOP 
        inner JOIN 
        	tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
        WHERE
        	r.Empresa = 1 and r.codFase in (429, 431, 455, 459) and r.dataEmissao >= '""" + self.data_inicio + """'and r.dataEmissao <=  '""" + self.data_final + """'"""
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                realizado = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        realizado['categoria'] = '-'
        realizado['nome'] = realizado['nome'].astype(str)
        faccionistas['codfaccionista'] = faccionistas['codfaccionista'].astype(str)

        realizado['categoria'] = realizado['nome'].apply(self.mapear_categoria)

        realizado = pd.merge(realizado,faccionistas,on='codfaccionista',how='left')
        realizado.fillna('-',inplace=True)



        return realizado

