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
def get_OpsMaesAntesMontagem():

    dados = GP.GestaoPartes('','426','425').validarAguardandoPartesOPMae()

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


@GestaoPartes_routes.route('/pcp/api/EstoquePartes', methods=['GET'])
def get_EstoquePartes():
    filtrarConciliacao = request.args.get('filtrarConciliacao',False)

    dados = GP.GestaoPartes('','426','425').obtendoEstoquePartesNat20(bool(filtrarConciliacao))

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


@GestaoPartes_routes.route('/pcp/api/DetalhaGradeOPMae', methods=['GET'])
def get_DetalhaGradeOPMae():

    dados = GP.GestaoPartes('','426','425').detalharOPMaeGrade()

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

@GestaoPartes_routes.route('/pcp/api/OPposMotagem', methods=['GET'])
def get_OPposMotagem():

    dados = GP.GestaoPartes('','426','425').ordemProdAbertoPosMontagem()

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


@GestaoPartes_routes.route('/pcp/api/EstoqueFuturoNat20', methods=['GET'])
def get_EstoqueFuturoNat20():

    dados = GP.GestaoPartes('','426','425').EstoqueProgramadonatureza20()

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

@GestaoPartes_routes.route('/pcp/api/EstuturaPartes', methods=['GET'])
def get_EstuturaPartes():

    dados = GP.GestaoPartes('','426','425').estruturaItensPartes()

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





