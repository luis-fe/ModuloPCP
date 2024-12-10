from flask import Blueprint, jsonify, request
from functools import wraps
from models.MonitorPedidos import monitorOP, AutomacaoOPs
from models import MonitorPedidosOPsClass
import threading  # Para rodar o terminate em uma thread separada

MonitorOp_routes = Blueprint('MonitorOp_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@MonitorOp_routes.route('/pcp/api/monitorOPs', methods=['GET'])
@token_required
def get_monitorOPs():
    dataInico = request.args.get('dataInico', '-')
    dataFim = request.args.get('dataFim')

    empresa = 1
    monitor = MonitorPedidosOPsClass.MonitorPedidosOps(empresa, dataInico, dataFim, None, dataInico, dataFim, None,
                                                       None, None, None, None, None)
    dados = monitor.gerarMonitorOps(dataInico, dataFim)

    # Obtém os nomes das colunas
    column_names = dados.columns

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)

    # Retorna os dados JSON para o cliente
    response = jsonify(OP_data)

    # Após retornar a resposta, reiniciar o app em uma nova thread
    #porta_atual = 8000  # Substitua pela porta correta que você está utilizando
    #thread = threading.Thread(target=monitor.reiniciandoAPP(), args=(porta_atual,))
    #thread.start()
    return response


@MonitorOp_routes.route('/pcp/api/monitorOPsFiltroPedidos', methods=['POST'])
@token_required
def post_monitorOPsFiltroPedidos():
    data = request.get_json()

    dataInico = data.get('dataInico')
    dataFim = data.get('dataFim')
    arrayPedidos = data.get('arrayPedidos')

    empresa = 1
    monitor = MonitorPedidosOPsClass.MonitorPedidosOps(empresa, dataInico, dataFim, None, dataInico, dataFim, None,
                                                       None, None, None, None, None)
    dados = monitor.filtrandoPedido(arrayPedidos, dataInico, dataFim)

    # Obtém os nomes das colunas
    column_names = dados.columns

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)

    # Retorna os dados JSON para o cliente
    response = jsonify(OP_data)

    # Após retornar a resposta, reiniciar o app em uma nova thread
    #porta_atual = 8000  # Substitua pela porta correta que você está utilizando
    #thread = threading.Thread(target=monitor.reiniciandoAPP(), args=(porta_atual,))
    #thread.start()
    return response



@MonitorOp_routes.route('/pcp/api/DelhalamentoMonitorOP', methods=['GET'])
@token_required
def get_DelhalamentoOPMonitor():
    numeroOP = request.args.get('numeroOP', '-')
    dataInico = request.args.get('dataInico', '-')
    dataFim = request.args.get('dataFim', '-')

    dados = monitorOP.DetalhaOPMonitor(numeroOP,dataInico, dataFim)
    # Converte o DataFrame em uma lista de dicionários
    OP_data = dados.to_dict(orient='records')

    return jsonify(OP_data)

@MonitorOp_routes.route('/pcp/api/ProdutosSemOP', methods=['GET'])
@token_required
def get_ProdutosSemOP():
    dados = monitorOP.ProdutosSemOP()
    # Converte o DataFrame em uma lista de dicionários
    OP_data = dados.to_dict(orient='records')

    return jsonify(OP_data)

@MonitorOp_routes.route('/pcp/api/ProdutosSemOP', methods=['POST'])
@token_required
def POST_ProdutosSemOP():
    data = request.get_json()
    dataInico = data.get('dataInico', '-')
    dataFim = data.get('dataFim', '-')
    dados = MonitorPedidosOPsClass.MonitorPedidosOps('1' , dataInico, dataFim,None, dataInico, dataFim,None,None,None,None,None, None).produtosSemOP_()

    # Converte o DataFrame em uma lista de dicionários
    OP_data = dados.to_dict(orient='records')

    return jsonify(OP_data)


@MonitorOp_routes.route('/pcp/api/Op_tam_cor', methods=['POST'])
@token_required
def post_Op_tam_cor():
    data = request.get_json()

    dataInico = data.get('dataInico')
    dataFim = data.get('dataFim')

    empresa = 1
    monitor = MonitorPedidosOPsClass.MonitorPedidosOps(empresa, dataInico, dataFim, None, dataInico, dataFim, None,
                                                       None, None, None, None, None)
    dados = monitor.ops_tamanho_cor()

    # Obtém os nomes das colunas
    column_names = dados.columns

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)

    # Retorna os dados JSON para o cliente
    response = jsonify(OP_data)

    # Após retornar a resposta, reiniciar o app em uma nova thread
    #porta_atual = 8000  # Substitua pela porta correta que você está utilizando
    #thread = threading.Thread(target=monitor.reiniciandoAPP(), args=(porta_atual,))
    #thread.start()
    return response


@MonitorOp_routes.route('/pcp/api/Op_tam_corPedidos', methods=['POST'])
@token_required
def post_Op_tam_corPedidos():
    data = request.get_json()

    dataInico = data.get('dataInico')
    dataFim = data.get('dataFim')
    arrayPedidos = data.get('arrayPedidos')


    empresa = 1
    monitor = MonitorPedidosOPsClass.MonitorPedidosOps(empresa, dataInico, dataFim, None, dataInico, dataFim, None,
                                                       None, None, None, None, None)
    dados = monitor.ops_tamanho_cor_Pedidos(arrayPedidos)

    # Obtém os nomes das colunas
    column_names = dados.columns

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)

    # Retorna os dados JSON para o cliente
    response = jsonify(OP_data)

    # Após retornar a resposta, reiniciar o app em uma nova thread
    #porta_atual = 8000  # Substitua pela porta correta que você está utilizando
    #thread = threading.Thread(target=monitor.reiniciandoAPP(), args=(porta_atual,))
    #thread.start()
    return response
