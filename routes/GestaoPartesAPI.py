'''
CONTROLE DAS APIS REQUISITADAS QUE INTERAGEM COM AS CLASSES :
GESTAOPARTES
'''

from flask import Blueprint, jsonify, request
from functools import wraps
from models import GestaoPartes as GP

GestaoPartes_routes = Blueprint('GestaoPartes_routes', __name__)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@GestaoPartes_routes.route('/pcp/api/OpsMaesAntesMontagem', methods=['GET'])
@token_required
def get_OpsMaesAntesMontagem():

    dados = GP.GestaoPartes().validarAguardandoPartesOPMae()

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








