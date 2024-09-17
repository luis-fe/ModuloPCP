from flask import Blueprint, jsonify, request
from functools import wraps
from models.MonitorPedidos import monitorOP, AutomacaoOPs

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
    dataInico = request.args.get('dataInico','-')
    dataFim = request.args.get('dataFim')

    #controle.InserindoStatus(rotina, ip, datainicio)
    dados = monitorOP.ReservaOPMonitor(dataInico , dataFim)


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
    dados = monitorOP.ProdutosSemOP_(dataInico,dataFim)
    # Converte o DataFrame em uma lista de dicionários
    OP_data = dados.to_dict(orient='records')

    return jsonify(OP_data)

