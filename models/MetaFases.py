from connection import ConexaoPostgreWms


class MetaFases():
    def __init__(self, codPlano = None, codLote = None):

        self.codPlano = codPlano
        self.codLote = codLote
def metasFase(Codplano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado = False):
    '''Metodo que consulta as meta por fase'''

    nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
    novo = ", ".join(nomes_com_aspas)
    conn = ConexaoPostgreWms.conexaoEngine()

    if congelado == False:


        sqlMetas = """
        SELECT "codLote", "Empresa", "codEngenharia", "codSeqTamanho", "codSortimento", previsao
        FROM "PCP".pcp.lote_itens li
        WHERE "codLote" IN (%s)
        """ % novo