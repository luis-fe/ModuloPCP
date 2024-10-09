'''
CONTROLE DAS APIS REQUISITADAS QUE INTERAGEM COM AS CLASSES :
FACCIONISTA E FACCIONISTACATEGORIA , PRESENTE NO DIRETORIO MODELS
'''

from flask import Blueprint, jsonify, request
from functools import wraps
from models import Faccionista as fac

FaccionostaAPI_routes = Blueprint('FaccionostaAPI_routes', __name__)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@FaccionostaAPI_routes.route('/pcp/api/ConsultaFaccionistasCsw', methods=['GET'])
@token_required
def get_ConsultaFaccionistasCsws():

    dados = fac.Faccionista().ListaFaccionistasCsw()

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


@FaccionostaAPI_routes.route('/pcp/api/RegistroFaccionistas', methods=['GET'])
@token_required
def get_RegistroFaccionistas():

    faccionista = fac.Faccionista()
    dados = faccionista.RegistroFaccionistas()

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


