'''
MODULO FEITO PARA CROD DO PLANO - CONJUNTO DE REGRAS QUE FORMAM A POLITICA A SER PROJETADA E SIMULADA
'''

from connection import ConexaoPostgreWms, ConexaoBanco
from datetime import datetime
import pandas as pd

def ObeterPlanos():
    conn = ConexaoPostgreWms.conexaoEngine()
    planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)
    planos.rename(
        columns={'codigo': '01- Codigo Plano', 'descricao do Plano': '02- Descricao do Plano', 'inicioVenda': '03- Inicio Venda',
                 'FimVenda': '04- Final Venda', "inicoFat": "05- Inicio Faturamento", "finalFat": "06- Final Faturamento",
                 'usuarioGerador': '07- Usuario Gerador', 'dataGeracao': '08- Data Geracao'},
        inplace=True)
    planos.fillna('-', inplace=True)

    sqlLoteporPlano = """
    select
        plano as "01- Codigo Plano",
        lote,
        nomelote
    from
        "PCP".pcp."LoteporPlano"
    """
    lotes = pd.read_sql(sqlLoteporPlano, conn)

    lotes['01- Codigo Plano'] = lotes['01- Codigo Plano'].astype(str)

    merged = pd.merge(planos, lotes, on='01- Codigo Plano', how='left')

    # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
    grouped = merged.groupby(['01- Codigo Plano', '02- Descricao do Plano', '03- Inicio Venda', '04- Final Venda',
                              '05- Inicio Faturamento', '06- Final Faturamento', '07- Usuario Gerador', '08- Data Geracao']).agg({
        'lote': lambda x: list(x.dropna().astype(str)),
        'nomelote': lambda x: list(x.dropna().astype(str))
    }).reset_index()

    result = []
    for index, row in grouped.iterrows():
        entry = {
            '01- Codigo Plano': row['01- Codigo Plano'],
            '02- Descricao do Plano': row['02- Descricao do Plano'],
            '03- Inicio Venda': row['03- Inicio Venda'],
            '04- Final Venda': row['04- Final Venda'],
            '05- Inicio Faturamento': row['05- Inicio Faturamento'],
            '06- Final Faturamento': row['06- Final Faturamento'],
            '07- Usuario Gerador': row['07- Usuario Gerador'],
            '08- Data Geracao': row['08- Data Geracao'],
            'lotes': row['lote'],
            'nomelote': row['nomelote']
        }
        result.append(entry)

    return result

