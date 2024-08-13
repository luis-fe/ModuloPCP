import gc
import pytz
import numpy as np
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
from models.Planejamento import SaldoPlanoAnterior, itemsPA_Csw, plano
from datetime import datetime

def AnaliseDeMateriais(codPlano, codLote, congelado):
    conn = ConexaoPostgreWms.conexaoEngine()
    if congelado == False:

        # Obtendo as Previsao do Lote
        sqlMetas = """
        SELECT "codLote", "Empresa", "codEngenharia", "codSeqTamanho"::varchar, "codSortimento"::varchar, previsao
        FROM "PCP".pcp.lote_itens li
        WHERE "codLote" = %s
        """
        sqlMetas = pd.read_sql(sqlMetas,conn, params=(codLote,))

        consulta = """
               select codigo as "codItem", nome, "unidadeMedida" , "codItemPai" , "codSortimento"::varchar as "codSortimento" , "codSeqTamanho"::varchar as "codSeqTamanho"  from pcp.itens_csw ic 
               """
        consulta = pd.read_sql(consulta, conn)
        # Verificar quais codItemPai começam com '1' ou '2'
        mask = consulta['codItemPai'].str.startswith(('1', '2'))
        # Aplicar as transformações usando a máscara
        consulta['codEngenharia'] = np.where(mask, '0' + consulta['codItemPai'] + '-0', consulta['codItemPai'] + '-0')

        sqlMetas = pd.merge(sqlMetas, consulta, on=["codEngenharia", "codSeqTamanho", "codSortimento"], how='left')
        sqlMetas['codItem'].fillna('-', inplace=True)
        saldo = SaldoPlanoAnterior.SaldosAnterior(codPlano)
        sqlMetas = pd.merge(sqlMetas, saldo, on='codItem', how='left')

        faturado = SaldoPlanoAnterior.FaturamentoPlano(codPlano)
        sqlMetas = pd.merge(sqlMetas, faturado, on='codItem', how='left')

        estoque, cargas = itemsPA_Csw.EstoquePartes()
        sqlMetas = pd.merge(sqlMetas, estoque, on='codItem', how='left')

        # cargas = itemsPA_Csw.CargaFases()
        sqlMetas = pd.merge(sqlMetas, cargas, on='codItem', how='left')

        sqlMetas.fillna({
            'saldo': 0,
            'qtdeFaturada': 0,
            'estoqueAtual': 0,
            'carga': 0
        }, inplace=True)

        # Analisando se esta no periodo de faturamento
        diaAtual = datetime.strptime(obterDiaAtual(), '%Y-%m-%d')

        planoAtual = plano.ConsultaPlano()
        planoAtual = planoAtual[planoAtual['codigo'] == codPlano].reset_index()

        # Levantar as data de início e fim do faturamento:
        IniFat = datetime.strptime(planoAtual['inicoFat'][0], '%Y-%m-%d')

        if diaAtual >= IniFat:
            sqlMetas['FaltaProgramar1'] = sqlMetas['previsao'] - (
                        sqlMetas['estoqueAtual'] + sqlMetas['carga'] + sqlMetas['qtdeFaturada'])
        else:
            sqlMetas['estoque-saldoAnt'] = sqlMetas['estoqueAtual'] - sqlMetas['saldo']
            sqlMetas['FaltaProgramar1'] = sqlMetas['previsao'] - (sqlMetas['estoque-saldoAnt'] + sqlMetas['carga'])
        try:
            sqlMetas['FaltaProgramar'] = sqlMetas.apply(
                lambda l: l['FaltaProgramar1'] if l['FaltaProgramar1'] > 0 else 0, axis=1)
        except:
            print('verificar')

        #Obtendo os consumos de todos os componentes relacionados nas engenharias
        sqlcsw = """
        SELECT v.codProduto as codEngenharia, cv.codSortimento, cv.seqTamanho as codSeqTamanho,  v.CodComponente,
        (SELECT i.nome FROM cgi.Item i WHERE i.codigo = v.CodComponente) as descricaoComponente,
        (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
        cv.quantidade  from tcp.ComponentesVariaveis v 
        join tcp.CompVarSorGraTam cv on cv.codEmpresa = v.codEmpresa and cv.codProduto = v.codProduto and cv.sequencia = v.codSequencia 
        WHERE v.codEmpresa = 1
        and v.codProduto in (select l.codengenharia from tcl.LoteSeqTamanho l WHERE l.empresa = 1 and l.codlote = '"""+codLote+"""')
        and v.codClassifComponente <> 12
        UNION 
        SELECT v.codProduto as codEngenharia,  l.codSortimento ,l.codSeqTamanho as codSeqTamanho, v.CodComponente,
        (SELECT i.nome FROM cgi.Item i WHERE  i.codigo = v.CodComponente) as descricaoComponente,
        (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
        v.quantidade  from tcp.ComponentesPadroes  v 
        join tcl.LoteSeqTamanho l on l.Empresa = v.codEmpresa and l.codEngenharia = v.codProduto and l.codlote = '"""+codLote+"""'"""

        sqlEstoque = """
        SELECT
	    d.codItem as CodComponente ,
	    d.estoqueAtual
        FROM
	    est.DadosEstoque d
        WHERE
	    d.codEmpresa = 1
	    and d.codNatureza in (1, 3, 2)
	    and d.estoqueAtual > 0
        """

        sqlRequisicaoAberto = """
        SELECT
	ri.codMaterial as CodComponente ,
	ri.qtdeRequisitada as EmRequisicao
    FROM
	tcq.RequisicaoItem ri
    join tcq.Requisicao r on
	r.codEmpresa = ri.codEmpresa
	and r.numero = ri.codRequisicao
    where
	ri.codEmpresa = 1
	and r.sitBaixa <0
        """

        sqlAtendidoParcial = """
        SELECT
		i.codPedido as pedCompra,
		i.codPedidoItem as seqitem,
		i.quantidade as qtAtendida
	FROM
		Est.NotaFiscalEntradaItens i
	WHERE
		i.codempresa = 1 
		and i.codPedido >0 
		and codPedido in (select codpedido FROM
	sup.PedidoCompraItem p
WHERE
	p.situacao in (0, 2))
        """

        sqlPedidos = """
        SELECT
	p.codPedido pedCompra,
	p.codProduto as CodComponente,
	p.quantidade as qtdPedida,
	p.dataPrevisao,
	p.itemPedido as seqitem,
	p.situacao
from 
	sup.PedidoCompraItem p
WHERE
	p.situacao in (0, 2)
	and p.codEmpresa = 1
        """


        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlcsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consumo = pd.DataFrame(rows, columns=colunas)

                cursor.execute(sqlEstoque)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlEstoque = pd.DataFrame(rows, columns=colunas)

                cursor.execute(sqlRequisicaoAberto)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlRequisicaoAberto = pd.DataFrame(rows, columns=colunas)
                sqlRequisicaoAberto = sqlRequisicaoAberto.groupby(["CodComponente"]).agg(
                    {"EmRequisicao": "sum"}).reset_index()

                cursor.execute(sqlAtendidoParcial)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlAtendidoParcial = pd.DataFrame(rows, columns=colunas)

                cursor.execute(sqlPedidos)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                sqlPedidos = pd.DataFrame(rows, columns=colunas)

                sqlPedidos = pd.merge(sqlPedidos,sqlAtendidoParcial,on=['pedCompra','seqitem'],how='left')
                sqlPedidos['qtAtendida'].fillna(0,inplace=True)
                sqlPedidos['QtdSaldo'] = sqlPedidos['qtdPedida'] - sqlPedidos['qtAtendida']
                sqlPedidos = sqlPedidos.groupby(["CodComponente"]).agg(
                    {"QtdSaldo": "sum"}).reset_index()

        # Libera memória manualmente
        del rows
        gc.collect()

        Necessidade = pd.merge(sqlMetas, consumo, on=["codEngenharia" , "codSeqTamanho" , "codSortimento"], how='left')

        Necessidade['quantidade'].fillna(0,inplace=True)
        Necessidade['quantidade Prevista'] = Necessidade['quantidade'] * Necessidade['previsao']
        Necessidade["FaltaProgramarMeta"] = Necessidade['quantidade'] * Necessidade['FaltaProgramar']
        Necessidade = Necessidade.groupby(["CodComponente"]).agg({"descricaoComponente":"first","quantidade Prevista":"sum","FaltaProgramarMeta":"sum","unid":"first"}).reset_index()
        Necessidade = pd.merge(Necessidade, sqlEstoque, on=["CodComponente"], how='left')
        Necessidade = pd.merge(Necessidade, sqlRequisicaoAberto, on=["CodComponente"], how='left')
        Necessidade = pd.merge(Necessidade, sqlPedidos, on=["CodComponente"], how='left')
        Necessidade['QtdSaldo'].fillna(0,inplace=True)


        return Necessidade
def obterDiaAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%Y-%m-%d')
    return agora