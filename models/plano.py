'''MODULO FEITO PARA CROD DO PLANO - CONJUNTO DE REGRAS QUE FORMAM A POLITICA A SER PROJETADA E SIMULADA'''

from ..connection import ConexaoPostgreWms
from datetime import datetime
import pandas as pd


def ObeterPlanos(host, bancoDados):
    conn = ConexaoPostgreWms.conexaoEngine(host, bancoDados)
    planos = pd.read_sql('SELECT * FROM pcp."Plano" '
                         ' ORDER BY codigo ASC;',conn)
    planos.rename(
        columns={'codigo': '01- Codigo Plano', 'descricao do Plano': '02- Descricao do Plano', 'inicioVenda': '03- Inicio Venda',
                 'FimVenda':'04- Final Venda',"inicoFat":"05- Inicio Faturamento","finalFat":"06- Final Faturamento",
                 'usuarioGerador':'07- Usuario Gerador','dataGeracao':'08- Data Geracao'},
        inplace=True)
    planos.fillna('-', inplace=True)

    return planos



