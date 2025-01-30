from models import TendenciasPlano


class AnaliseDistribuicaoMPSKU():
    '''Classe utilizada para distribuir o material nos sku'''

    def __init__(self, empresa = None, codReduzido = None, codComponente = None, percentualIntervalado = None, codPlano = None,  consideraBloqueado = None):

        self.empresa = empresa
        self.codReduzido = codReduzido
        self.codComponente = codComponente
        self.percentualIntervalado = percentualIntervalado
        self.codPlano = codPlano
        self.consideraBloqueado = consideraBloqueado

    def fila_sku(self, simula = 'nao'):

        sqlMetas = TendenciasPlano.TendenciaPlano(self.codPlano, self.consideraBloqueado).tendenciaVendas(simula)

        sqlMetas['filaPecas1'] = sqlMetas['previcaoVendas']
        sqlMetas['filaPecas2'] = sqlMetas['previcaoVendas'] - sqlMetas['qtdePedida']

        # Transformando o DataFrame
        df_long = sqlMetas.melt(id_vars=["produto"], value_vars=["venda1", "venda2"], var_name="filaPecas", value_name="fila")

        # Removendo a coluna 'venda' pois não é mais necessária
        df_long = df_long.drop(columns=["filaPecas"])

