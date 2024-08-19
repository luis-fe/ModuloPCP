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
            rf."totPecasOPBaixadas as "Realizado"
        FROM
            "PCP".pcp.realizado_fase rf 
        WHERE
            rf."dataBaixa"::date >= %s AND rf."dataBaixa"::date <= %s 
        """

        # Consulta SQL para obter os dados de entrada
        sql_entrada = """
        SELECT
            rf.numeroop,
            rf."seqRoteiro",
            rf."dataBaixa"        
        FROM
            "PCP".pcp.realizado_fase rf
        WHERE
            rf."dataBaixa"::date >= current_date - interval '30 days';
        """

        try:
            # Conectar ao banco de dados
            conn = ConexaoPostgreWms.conexaoEngine()

            # Executar as consultas
            saida = pd.read_sql(sql, conn, params=(self.data_inicio, self.data_final))
            entrada = pd.read_sql(sql_entrada, conn)

            # Processar os dados
            entrada['seqRoteiro'] = entrada['seqRoteiro'] + 1
            entrada.rename(columns={'dataBaixa': 'dataEntrada'}, inplace=True)
            saida = pd.merge(saida, entrada, on=['numeroop', 'seqRoteiro'])

            # Verifica e converte para datetime se necessário
            saida['dataEntrada'] = pd.to_datetime(saida['dataEntrada'], errors='coerce')
            saida['dataBaixa'] = pd.to_datetime(saida['dataBaixa'], errors='coerce')
            saida['LeadTime(diasCorridos)'] = (saida['dataBaixa'] - saida['dataEntrada']).dt.days

            TotalPecas = saida['Realizado'].sum()

            return saida

        except Exception as e:
            print(f"Erro ao calcular o Lead Time: {e}")
            return None

