import numpy as np

from connection import ConexaoPostgreWms
import pandas as pd

from models import FaturamentoClass, FaseClass
from models.Planejamento import itemsPA_Csw


class MetaFases():
    '''Classe utilizada para construcao das metas por fase a nivel departamental '''
    def __init__(self, codPlano = None, codLote = None):

        self.codPlano = codPlano
        self.codLote = codLote
def metasFase(Codplano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado = False):
    '''Metodo que consulta as meta por fase'''

    # 1 - obtendo o codigo do lote a ser considerado
    nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
    novo = ", ".join(nomes_com_aspas)

    # 1.1 - Abrindo a conexao com o Banco
    conn = ConexaoPostgreWms.conexaoEngine()

    # 2.0 - Verificando se o usuario está analisando em congelamento , CASE NAO:
    if congelado == False:

        # 2.1 sql para pesquisar a previsao a nivel de cor e tam, de acordo com o lote escolhido:

        sqlMetas = """
            SELECT 
                "codLote", "Empresa", "codEngenharia", "codSeqTamanho", "codSortimento", previsao
            FROM 
                "PCP".pcp.lote_itens li
            WHERE 
                "codLote" IN (%s)
        """ % novo

        # 2.2 - Sql que obtem os roteiros das engenharias
        sqlRoteiro = """
            select 
                * 
            from 
                "PCP".pcp."Eng_Roteiro" er 
        """

        # 2.3 Sql que obtem a ordem de apresentacao de cada fase
        sqlApresentacao = """
        select 
            "nomeFase" , 
            apresentacao  
        from "PCP".pcp."SeqApresentacao" sa 
        """

        # 2.4 Sql utilizado como apoio para obter a meta a nivel de cor , tamanho, categoria
        consulta = """
        select 
            codigo as "codItem", nome, "unidadeMedida" , "codItemPai" , "codSortimento" as "codSortimento" , "codSeqTamanho" as "codSeqTamanho" , categoria 
        from pcp.itens_csw ic 
        """


        sqlMetas = pd.read_sql(sqlMetas,conn)
        sqlRoteiro = pd.read_sql(sqlRoteiro,conn)
        sqlApresentacao = pd.read_sql(sqlApresentacao,conn)

        consulta = pd.read_sql(consulta, conn)

        # Verificar quais codItemPai começam com '1' ou '2'
        mask = consulta['codItemPai'].str.startswith(('1', '2'))
        # Aplicar as transformações usando a máscara
        consulta['codEngenharia'] = np.where(mask, '0' + consulta['codItemPai'] + '-0', consulta['codItemPai'] + '-0')

        sqlMetas = pd.merge(sqlMetas, consulta, on=["codEngenharia", "codSeqTamanho", "codSortimento"], how='left')
        sqlMetas['codItem'].fillna('-', inplace=True)


        # 3 - Obter o faturamento de um determinado plano e aplicar ao calculo
        faturado = FaturamentoClass.Faturamento(None,None,None,Codplano)
        faturadoPeriodo = faturado.faturamentoPeriodo_Plano()
        faturadoPeriodoPartes = faturado.faturamentoPeriodo_Plano_PartesPeca()
        faturadoPeriodo = pd.concat([faturadoPeriodo, faturadoPeriodoPartes], ignore_index=True)

        sqlMetas = pd.merge(sqlMetas,faturadoPeriodo,on='codItem',how='left')

        # 4 - Aplicando os estoques ao calculo
        #----------------------------------------------------------------------------------------------------------
        estoque = itemsPA_Csw.EstoquePartes()
        sqlMetas = pd.merge(sqlMetas, estoque, on='codItem', how='left')




        # 5- Aplicando a carga em producao
        #-------------------------------------------------------------------------------------------------------
        cargaFases = FaseClass.FaseProducao()
        cargas = cargaFases.cargaPartes()

        sqlMetas = pd.merge(sqlMetas, cargas, on='codItem', how='left')

        sqlMetas.fillna({
            'saldo': 0,
            'qtdeFaturada': 0,
            'estoqueAtual': 0,
            'carga': 0
        }, inplace=True)
        #--------------------------------------------------------------------------------------------------------


