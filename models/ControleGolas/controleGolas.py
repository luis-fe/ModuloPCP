import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms


def ControleGolasPunhos():

    # Passo 1: retirando os itens movimentados para a natureza de conferencia
    sql = """
    SELECT TOP 1000 m.dataMovto as dataEntrada  , m.codItem, m.numDocto, m.qtdMovto as Qtd_Entrada, m.codUnidEstoque, m.nomeItem  FROM est.Movimento m
    WHERE m.codEmpresa = 1 and m.codTransacao = 200 and (nomeItem like '%GOLA%' OR nomeItem like '%PUNHO%' ) 
    ORDER BY dataLcto DESC 
    """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sql)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            EntradaConferencia = pd.DataFrame(rows, columns=colunas)
            del rows, colunas


    #Passo2 : Itens movimentados como SAIDA na natureza de conferencia
    sql2 = """
    SELECT TOP 1000 m.dataMovto as dataSaida , m.codItem, m.numDocto, - m.qtdMovto AS Qtd_saida  FROM est.Movimento m
    WHERE m.codEmpresa = 1 and m.codTransacao IN ( 1651, 1659) and (nomeItem like '%GOLA%' OR nomeItem like '%PUNHO%' )
    ORDER BY dataLcto DESC 
    """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sql2)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            SaidaConferencia = pd.DataFrame(rows, columns=colunas)
            del rows, colunas

    #Passo2.1 - retirando a expressao "/ET" do numero da nota

    SaidaConferencia['numDocto'] = SaidaConferencia['numDocto'].str.replace('/ET','')

    #Passo3 - fazendo o merge item + nota

    conferencia = pd.merge(EntradaConferencia, SaidaConferencia, on=['numDocto','codItem'], how='left')
    conferencia.fillna('-',inplace=True)

    #Passo4 - lendo o estoque da conferencia

    sql3 = """
    SELECT d.codItem , d.estoqueAtual  FROM est.DadosEstoque d
    WHERE d.codNatureza = 16 and d.codEmpresa = 1 and d.estoqueAtual > 0
    """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sql3)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            estoque = pd.DataFrame(rows, columns=colunas)
            del rows, colunas

    conferencia = pd.merge(conferencia,estoque,on='codItem', how='left')
    conferencia.fillna('-',inplace=True)

    conferencia['Chave1'] = conferencia['dataEntrada'].str.slice(8, 10).astype(int)
    conferencia['Chave']  = (conferencia['Chave1'] + 7).astype(str)
    conferencia['Chave'] = conferencia['dataEntrada'].str.slice(0, 7) +'_'+conferencia['Chave']  +'-'+conferencia['codItem']

    return conferencia



def FuncaoObterEtiquetasGolas():
    sql = """
    -- SQL BUSCAR ETIQUETAS DE ROLO DAS GOLAS E PUNHOS
SELECT top 40000 s.* , i.nome, DATEDIFF(day, '1840-12-31', dataMovimento) AS codbarras FROM Ppcpt_Tin_Ttp.EtiquetasProdCorSimula s
inner join cgi.Item i on i.codigo = s.codProdutoRed 
WHERE s.codempresa = 1
and (i.nome like '%GOLA%' OR i.nome like '%PUNHO%') AND (s.codProduto not like '2501%' and s.codProduto not like '2502%') --and codprodutored = '4366'
order by s.dataMovimento desc 
    """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sql)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            etiquetas = pd.DataFrame(rows, columns=colunas)
            del rows, colunas

    etiquetas['codbarras'] = etiquetas['codbarras'].astype(str)
    etiquetas['seqPesagem'] = etiquetas['seqPesagem'].astype(str)

    etiquetas['NumeroDeCaracteres'] = etiquetas['seqPesagem'].str.len()

    etiquetas['Zeros'] = etiquetas['NumeroDeCaracteres'].apply(lambda x: '0' * x)
    return etiquetas