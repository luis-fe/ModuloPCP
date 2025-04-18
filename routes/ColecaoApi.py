'''
CONTROLE DAS APIS REQUISITADAS QUE INTERAGEM COM AS CLASSE COLECAO
'''

from flask import Blueprint, jsonify, request
from functools import wraps
from models import Colecao as colec

ColecaoAPI_routes = Blueprint('ColecaoAPI_routes', __name__)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@ColecaoAPI_routes.route('/pcp/api/colecao_csw', methods=['GET'])
@token_required
def get_colecao_csw():


    dados = colec.Colecao().obterColecaoCsw()
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


@ColecaoAPI_routes.route('/pcp/api/VincularColecoesPlano', methods=['POST'])
@token_required
def post_VincularColecoesPlano():

    data = request.get_json()

    arrayColecao = data.get('arrayColecao')
    codPlano = data.get('codPlano')


    dados = colec.Colecao(None,codPlano).vincularArrayColecaoPlano(arrayColecao)
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



@ColecaoAPI_routes.route('/pcp/api/ConsultaColecaoVinculados', methods=['GET'])
@token_required
def get_ConsultaColecaoVinculados():
    codPlano = request.args.get('codPlano')

    dados = colec.Colecao(None,codPlano).obterColecoesporPlano()
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


@ColecaoAPI_routes.route('/pcp/api/DesvincularColecoesPlano', methods=['DELETE'])
@token_required
def Delete_DesvincularColecoesPlano():

    data = request.get_json()

    arrayColecao = data.get('arrayColecao')
    codPlano = data.get('codPlano')


    dados = colec.Colecao(None,codPlano).desvincularArrayColecaoPlano(arrayColecao)
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

