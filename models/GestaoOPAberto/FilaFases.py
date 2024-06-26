import pandas as pd
from connection import ConexaoBanco, ConexaoPostgreWms
import numpy as np
import re
from models.GestaoOPAberto import PainelGestaoOP

def RoteiroOPsAberto():
    sqlCsw = """
SELECT numeroOP  , codSeqRoteiro, codFase  FROM tco.RoteiroOP r
WHERE r.codEmpresa = 1 and 
numeroOP in (SELECT numeroOP from tco.OrdemProd op WHERE op.codempresa = 1 and op.situacao = 3 and op.codFaseAtual not in  (1, 401))
    """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sqlCsw)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            roteiro = pd.DataFrame(rows, columns=colunas)
            del rows

    return roteiro



def FilaFases():

    sqlOrdemAbertoCsw = """
    SELECT op.codLote , codTipoOP , numeroOP, codSeqRoteiroAtual, lot.descricao as desLote from tco.OrdemProd op 
    inner join tcl.Lote lot on lot.codLote = op.codLote  
    WHERE op.codempresa = 1 and op.situacao = 3
    """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sqlOrdemAbertoCsw)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            sqlOrdemAbertoCsw = pd.DataFrame(rows, columns=colunas)
            del rows

    fila = RoteiroOPsAberto()
    fila = pd.merge(fila,sqlOrdemAbertoCsw,on='numeroOP')
    fila['codSeqRoteiroAtual'] =fila['codSeqRoteiroAtual'].astype(int)

    fila['Situacao'] = np.where(fila['codSeqRoteiroAtual'] == fila['codSeqRoteiro'], 'em processo', '-')
    fila['Situacao'] = np.where((fila['codSeqRoteiroAtual'] > fila['codSeqRoteiro']) & (fila['Situacao'] == '-'),
                                'produzido', fila['Situacao'])
    fila['Situacao'] = np.where((fila['codSeqRoteiroAtual'] < fila['codSeqRoteiro']) & (fila['Situacao'] == '-'),
                                'a produzir', fila['Situacao'])

    sql_nomeFases = """
    SELECT f.codFase , f.nome as fase FROM tcp.FasesProducao f
    WHERE f.codEmpresa = 1 
    """

    with ConexaoBanco.ConexaoInternoMPL() as conn:
        with conn.cursor() as cursor_csw:
            # Executa a primeira consulta e armazena os resultados
            cursor_csw.execute(sql_nomeFases)
            colunas = [desc[0] for desc in cursor_csw.description]
            rows = cursor_csw.fetchall()
            sql_nomeFases = pd.DataFrame(rows, columns=colunas)
            del rows

    fila = pd.merge(fila,sql_nomeFases,on='codFase')

    sqlBuscarPecas = """
    select o.numeroop as "numeroOP", sum(o.total_pcs) as pcs from pcp.ordemprod o 
    group by numeroop
    """

    conn2 = ConexaoPostgreWms.conexaoEngine()

    sqlBuscarPecas = pd.read_sql(sqlBuscarPecas, conn2)
    fila = pd.merge(fila,sqlBuscarPecas,on='numeroOP')




    return fila


def ApresentacaoFila(COLECAO):
    fila = FilaFases()
    fila['COLECAO'] = fila['desLote'].apply(TratamentoInformacaoColecao)
    fila['COLECAO'] = fila['COLECAO'] + ' ' + fila['desLote'].apply(extrair_ano)
    fila.fillna('-', inplace=True)
    print(fila)


    colecoes = FiltroColecao(COLECAO)


    if colecoes['COLECAO'][0] != '-':
        fila = pd.merge(fila, colecoes , on='COLECAO')


    fila_carga_atual = fila[fila['Situacao'] == 'em processo'].reset_index()
    fila_fila = fila[fila['Situacao'] == 'a produzir'].reset_index()

    fila = fila.groupby('codFase').agg({"fase": 'first'}).reset_index()

    fila_carga_atual = fila_carga_atual.groupby('codFase').agg({"pcs": 'sum'}).reset_index()
    fila_carga_atual.rename(columns={'pcs': 'Carga Atual'}, inplace=True)

    fila_fila = fila_fila.groupby('codFase').agg({"pcs": 'sum'}).reset_index()
    fila_fila.rename(columns={'pcs': 'Fila'}, inplace=True)

    fila = pd.merge(fila, fila_carga_atual, on='codFase')
    fila = pd.merge(fila, fila_fila, on='codFase')

    apresentacao_query = """
        SELECT x."nomeFase" as "fase", apresentacao 
        FROM pcp."SeqApresentacao" x
        ORDER BY x.apresentacao
    """

    conn2 = ConexaoPostgreWms.conexaoEngine()
    apresentacao = pd.read_sql(apresentacao_query, conn2)
    fila = pd.merge(fila, apresentacao, on='fase')
    fila = fila[(fila['codFase'] < 599)]

    fila['Carga Atual'] =fila['Carga Atual'].astype(int).round()
    fila['Fila'] =fila['Fila'].astype(int).round()




    return fila

def FiltrosFila(NomeFase):
    fila = FilaFases()
    filaFila = fila[fila['Situacao'] == 'a produzir'].reset_index()
    filaFila = filaFila[filaFila['fase'] == NomeFase].reset_index()

    return filaFila


def TratamentoInformacaoColecao(descricaoLote):
    if 'INVERNO' in descricaoLote:
        return 'INVERNO'
    elif 'PRI' in descricaoLote:
        return 'VERAO'
    elif 'ALT' in descricaoLote:
        return 'ALTO VERAO'
    elif 'VER' in descricaoLote:
        return 'VERAO'
    else:
        return 'ENCOMENDAS'


def extrair_ano(descricaoLote):
    match = re.search(r'\b2\d{3}\b', descricaoLote)
    if match:
        return match.group(0)
    else:
        return None

def FiltroColecao(colecao):
    # Transformando o array em um dataFrame
    if colecao == '' or colecao == '-':
        return pd.DataFrame([{'COLECAO':'-'}])
    else:
        df = pd.DataFrame(colecao, columns=['COLECAO'])
        print(colecao)
        return df

