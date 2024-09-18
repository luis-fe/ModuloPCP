from flask import Blueprint, jsonify, request,  send_from_directory
from functools import wraps
from models.MonitorPedidos import monitor, MonitorSimulacaoEncerrOP
from models import MonitorPedidosOPsClass
MonitorPedidos_routes = Blueprint('MonitorPedidos_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@MonitorPedidos_routes.route('/pcp/api/monitorPreFaturamento', methods=['GET'])
@token_required
def get_MonitorPedidos():
    empresa = request.args.get('empresa')
    iniVenda = request.args.get('iniVenda','-')
    finalVenda = request.args.get('finalVenda')
    tiponota = request.args.get('tiponota')
    parametroClassificacao = request.args.get('parametroClassificacao', 'DataPrevisao')  # Faturamento ou DataPrevisao
    tipoData = request.args.get('tipoData','DataEmissao') #DataEmissao x DataPrevOri
    arrayRepres_excluir = request.args.get('arrayRepres_excluir','')
    arrayRepre_Incluir = request.args.get('arrayRepre_Incluir','')
    nomeCliente = request.args.get('nomeCliente','')
    filtroDataEmissaoIni = request.args.get('FiltrodataEmissaoInicial','')
    filtroDataEmissaoFim = request.args.get('FiltrodataEmissaoFinal','')
    #controle.InserindoStatus(rotina, ip, datainicio)
    print(filtroDataEmissaoIni)
    dados = MonitorPedidosOPsClass.MonitorPedidosOps(empresa, iniVenda, finalVenda,tipoData, iniVenda, finalVenda,arrayRepres_excluir,arrayRepre_Incluir,nomeCliente,parametroClassificacao,filtroDataEmissaoIni, filtroDataEmissaoFim)\
        .resumoMonitor()
    #dados = monitor.API(empresa, iniVenda, finalVenda, tiponota,'rotina', 'ip', 'datainicio',parametroClassificacao, tipoData, arrayRepres_excluir, arrayRepre_Incluir, nomeCliente,FiltrodataEmissaoInicial ,FiltrodataEmissaoFinal)
    #controle.salvarStatus(rotina, ip, datainicio)

    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@MonitorPedidos_routes.route('/pcp/api/monitorPreFaturamento', methods=['POST'])
@token_required
def POST_MonitorPedidos():
    data = request.get_json()
    empresa = data.get('empresa', '-')

    # Parametros obrigatorios no POST
    iniVenda = data.get('iniVenda','-')
    finalVenda = data.get('finalVenda')
    FiltrodataEmissaoInicial = data.get('FiltrodataEmissaoInicial','')
    FiltrodataEmissaoFinal = data.get('FiltrodataEmissaoFinal','')



    parametroClassificacao = data.get('parametroClassificacao', 'DataPrevisao')  # Faturamento ou DataPrevisao
    tipoData = data.args.get('tipoData','DataEmissao') #DataEmissao x DataPrevOri

    # Parametros NAO OBRIGATORIOS NO POST (ESPECIAIS)
    # Array de tipo de nota
    tiponota = data.get('tiponota')

    # Array excluir codigo representante
    ArrayCodRepresExcluir = data.get('ArrayCodRepresExcluir','')

    # Array codrepresentante
    ArrayCodRepres = data.get('ArrayCodRepres','')

    # ARRAY NOME CLIENTE
    ArrayNomeCliente = data.get('ArrayNomeCliente','')

    #ARRAY CODIGO CLIENTE
    ArrayCodigoCliente= data.get('ArrayCodigoCliente','')

    #ARRAY CONCEITO CLIENTE
    ArrayConceitoCliente= data.get('ArrayConceitoCliente','')

    #ARRA REGIAO
    ArrayRegiao= data.get('ArrayRegiao','')


    #controle.InserindoStatus(rotina, ip, datainicio)
    dados = monitor.API(empresa, iniVenda, finalVenda, tiponota,'rotina', 'ip', 'datainicio',parametroClassificacao, tipoData, ArrayCodRepresExcluir, ArrayCodRepres, ArrayNomeCliente,FiltrodataEmissaoInicial ,FiltrodataEmissaoFinal)
    #controle.salvarStatus(rotina, ip, datainicio)

    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@MonitorPedidos_routes.route('/pcp/api/GerarMonitorCsv', methods=['GET'])
@token_required
def get_GerarMonitorCsv():
    iniVenda = request.args.get('iniVenda','-')
    finalVenda = request.args.get('finalVenda')

    monitor.ConversaoMonitor(iniVenda, finalVenda)
    descricaoArquivo = iniVenda+'_'+finalVenda

    return send_from_directory('./dados/', f'monitor{descricaoArquivo}.csv')


@MonitorPedidos_routes.route('/pcp/api/monitorPreFaturamentoSimulaOP', methods=['GET'])
@token_required
def get_monitorPreFaturamentoSimulaOP():
    empresa = request.args.get('empresa')
    iniVenda = request.args.get('iniVenda','-')
    finalVenda = request.args.get('finalVenda')
    tiponota = request.args.get('tiponota')
    parametroClassificacao = request.args.get('parametroClassificacao', 'DataPrevisao')  # Faturamento ou DataPrevisao
    tipoData = request.args.get('tipoData','DataEmissao') #DataEmissao x DataPrevOri
    arrayRepres_excluir = request.args.get('arrayRepres_excluir','')
    arrayRepre_Incluir = request.args.get('arrayRepre_Incluir','')
    ops = request.args.get('ops','')

    #controle.InserindoStatus(rotina, ip, datainicio)
    dados = MonitorSimulacaoEncerrOP.API(empresa, iniVenda, finalVenda, tiponota,'rotina', 'ip', 'datainicio',parametroClassificacao, tipoData, arrayRepres_excluir, arrayRepre_Incluir,ops)
    #controle.salvarStatus(rotina, ip, datainicio)

    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@MonitorPedidos_routes.route('/pcp/api/DetalhaPedidoMonitor', methods=['GET'])
@token_required
def get_DetalhaPedidoMonitor():

    codPedido = request.args.get('codPedido','')

    #controle.InserindoStatus(rotina, ip, datainicio)
    dados = monitor.DetalhaPedido(codPedido)
    #controle.salvarStatus(rotina, ip, datainicio)

    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)