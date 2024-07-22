import pandas as pd
from connection import ConexaoPostgreWms,ConexaoBanco
from models.Planejamento import plano

def SaldosAnterior(codigoPlano):
    planoAtual = plano.ConsultaPlano()
    planoAtual = planoAtual[planoAtual['codigo']==codigoPlano].reset_index()

    # 1 - Levantar a data de Inicio de vendas do plano atual
    IniVendas = planoAtual['inicioVenda'][0]


    #2 - Pedidos anteriores:
    sql = """
    SELECT  p."codPedido", p."codProduto" as "codItem", (p."qtdePedida" - p."qtdeFaturada" - p."qtdeFaturada") as saldo, p."codTipoNota", ic."codSortimento" as "codSortimento" , ic."codSeqTamanho"  as "codSeqTamanho"
    FROM "PCP".pcp."pedidosItemgrade" p 
    inner join pcp.itens_csw ic on ic.codigo  = p."codProduto" 
    WHERE "dataEmissao"::DATE >= %s ::date - INTERVAL '100 days';
    """

    conn = ConexaoPostgreWms.conexaoEngine()
    pedidos = pd.read_sql(sql,conn,params=(IniVendas,))


    # 3 - Filtrando os pedidos aprovados
    pedidosBloqueados = _PedidosBloqueados()
    pedidos = pd.merge(pedidos,pedidosBloqueados,on='codPedido',how='left')
    pedidos['situacaobloq'].fillna('Liberado',inplace=True)
    pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']


    #4 Filtrando somente os tipo de notas desejados

    sqlNotas = """
    select tnp."tipo nota" as "codTipoNota"  from "PCP".pcp."tipoNotaporPlano" tnp 
    where plano = %s
    """
    tipoNotas = pd.read_sql(sqlNotas,conn,params=(codigoPlano,))

    pedidos = pd.merge(pedidos,tipoNotas,on='codTipoNota')
    pedidos = pedidos.groupby("codItem").agg({"saldo":"sum"}).reset_index()
    pedidos = pedidos.sort_values(by=['saldo'], ascending=False)
    pedidos = pedidos[pedidos['saldo'] > 0].reset_index()
    return pedidos




def _PedidosBloqueados():
    consultacsw = """SELECT * FROM (
    SELECT top 300000 bc.codPedido, 'analise comercial' as situacaobloq  from ped.PedidoBloqComl  bc WHERE codEmpresa = 1  
    and bc.situacaoBloq = 1
    order by codPedido desc
    UNION 
    SELECT top 300000 codPedido, 'analise credito'as situacaobloq  FROM Cre.PedidoCreditoBloq WHERE Empresa  = 1  
    and situacao = 1
    order BY codPedido DESC) as D"""

    with ConexaoBanco.Conexao2() as conn:
        with conn.cursor() as cursor:
            cursor.execute(consultacsw)
            colunas = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            consulta = pd.DataFrame(rows, columns=colunas)

        del rows
        return consulta
