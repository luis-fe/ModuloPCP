from flask import Blueprint, jsonify, request
from functools import wraps
from models.MonitorPedidos import AutomacaoPedidos

AtualizaPedidosSku_routes = Blueprint('AtualizaPedidosSku', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@AtualizaPedidosSku_routes.route('/pcp/api/AtualizaPedidosSku', methods=['GET'])
@token_required
def get_AtualizaPedidosSku():

    dados = AutomacaoPedidos.IncrementarPedidos()
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
    del dados
    return jsonify(OP_data)