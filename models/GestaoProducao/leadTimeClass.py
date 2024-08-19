import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco

class LeadTime():
    def __init__(self, dataInicio, dataFinal):
        self.dataInicio = dataInicio
        self.dataFinal = dataFinal
    def ObterLeadTimeFases(self):
        sql = """
        select
	        rf.numeroop,
	        rf.codfase,
	        rf."seqRoteiro",
	        rf."dataBaixa" ,
	        rf."totPecasOPBaixadas"
        from
	        "PCP".pcp.realizado_fase rf 
	    where rf."dataBaixa"::date >= %s and rf."dataBaixa"::date <= %s 
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        Saida = pd.read_sql(sql,conn,params=(self.dataInicio, self.dataFinal))
        Entrada = pd.read_sql(sql, conn, params=(self.dataInicio, self.dataFinal))
        Entrada['seqRoteiro'] = Entrada['seqRoteiro'] +1
        Entrada.drop(['codfase','totPecasOPBaixadas'], axis=1, inplace=True)
        Entrada.rename(columns={'dataEntrada': 'dataBaixa'}, inplace=True)
        Saida = pd.merge(Saida,Entrada,on=['numeroop','seqRoteiro'])

        # Verifica e converte para datetime se necessário
        Saida['dataEntrada'] = pd.to_datetime(Saida['dataEntrada'], errors='coerce')
        Saida['dataBaixa'] = pd.to_datetime(Saida['dataBaixa'], errors='coerce')
        Saida['LeadTime(diasCorridos)'] = (Saida['dataEntrada'] - Saida['dataBaixa']).dt.days

        return Saida


