import gc

import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco

class LeadTimeCalculator:
    """
    Classe para calcular o Lead Time das fases de produção.

    Atributos:
        data_inicio (str): Data de início do intervalo para análise.
        data_final (str): Data final do intervalo para análise.
    """

    def __init__(self, data_inicio, data_final):
        """
        Inicializa a classe com o intervalo de datas para análise.

        Args:
            data_inicio (str): Data de início do intervalo.
            data_final (str): Data final do intervalo.
        """
        self.data_inicio = data_inicio
        self.data_final = data_final

    def obter_lead_time_fases(self):
        """
        Calcula o Lead Time para as fases de produção no intervalo especificado.

        Returns:
            pd.DataFrame: DataFrame contendo as informações de Lead Time por fase.
        """
        # Consulta SQL para obter os dados de saída
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

        # Consulta SQL para obter os dados de entrada NO CSW (maior velocidade de processamento))
        sql_entrada = """
                SELECT
                    o.codfase,
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

            # Libera memória manualmente
            del rows
            gc.collect()


            # Processar os dados
            entrada['seqRoteiro'] = entrada['seqRoteiro'] + 1
            entrada.rename(columns={'dataBaixa': 'dataEntrada'}, inplace=True)
            saida = pd.merge(saida, entrada, on=['numeroop', 'seqRoteiro'])
            saida = saida.drop_duplicates()

            # Verifica e converte para datetime se necessário
            saida['dataEntrada'] = pd.to_datetime(saida['dataEntrada'], errors='coerce')
            saida['dataBaixa'] = pd.to_datetime(saida['dataBaixa'], errors='coerce')
            saida['LeadTime(diasCorridos)'] = (saida['dataBaixa'] - saida['dataEntrada']).dt.days

            saida['RealizadoFase'] = saida.groupby('codfase')['Realizado'].transform('sum')
            saida['LeadTime(PonderadoPorQtd)'] = (saida['Realizado'] / saida['RealizadoFase']) * 100

            saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(diasCorridos)']*saida['LeadTime(PonderadoPorQtd)']
            saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(PonderadoPorQtd)'].round()

            saida = saida.groupby(["codfase"]).agg({"LeadTime(diasCorridos)": "mean", "Realizado": "sum",
                                                             "LeadTime(PonderadoPorQtd)": 'sum'}).reset_index()
            saida['LeadTime(PonderadoPorQtd)'] = saida['LeadTime(PonderadoPorQtd)']/100
            saida['LeadTime(diasCorridos)'] = saida['LeadTime(diasCorridos)'].round()
            return saida

        except Exception as e:
            print(f"Erro ao calcular o Lead Time: {e}")
            return None

