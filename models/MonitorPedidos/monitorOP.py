'''
Aqui utilizamos o monitor a nivel de OP , para melhor eficiencia na gestao de producao
'''

import pandas as pd
# Funcao para organizar o Monitor em datas de embarque (entrega) e atribuir a OP pré reservado para cada sku a nivel de pedido
def ReservaOPMonitor():
    # Passo 1 : Carregar os dados da OP
    consulta = """
    select o.codreduzido as "codProduto", id, "qtdAcumulada", "ocorrencia_sku" from "off".ordemprod o where "qtdAcumulada" > 0
    """
