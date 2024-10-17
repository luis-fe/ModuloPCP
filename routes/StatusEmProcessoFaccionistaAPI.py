'''
CONTROLE DAS APIS REQUISITADAS QUE INTERAGEM COM AS CLASSES :
StatusOPsEmProcesso , PRESENTE NO DIRETORIO MODELS
'''

from flask import Blueprint, jsonify, request
from functools import wraps
from models import StatusOpsEmProcesso as staOP
from models import Status as sta

StatusFaccionostaEmProcesso_routes = Blueprint('StatusFaccionostaEmProcesso_routes', __name__)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@StatusFaccionostaEmProcesso_routes.route('/pcp/api/FaccionistaCategoria', methods=['POST'])
@token_required
def post_FaccionistaCategoria():
    data = request.get_json()
    categoria = data.get('categoria',None)
    faccionista = data.get('faccionista', None)

    dados = staOP.StatusOpsEmProcesso(faccionista, None, None, None, None, None, None, categoria).getOPsEmProcessoCategoria()

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



@StatusFaccionostaEmProcesso_routes.route('/pcp/api/PesquisaOPFac', methods=['GET'])
@token_required
def get_PesquisaOPFac():
    numeroOP = request.args.get('numeroOP','-')

    dados = staOP.StatusOpsEmProcesso(None, None, numeroOP).filtrandoOPEspecifica()

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


@StatusFaccionostaEmProcesso_routes.route('/pcp/api/consultarStatusDisponiveis', methods=['GET'])
@token_required
def get_consultarStatusDisponiveis():

    dados = sta.StatusFac().consultarStatusDisponiveis()

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

@StatusFaccionostaEmProcesso_routes.route('/pcp/api/cadastrarStatus', methods=['POST'])
@token_required
def post_cadastrarStatus():
    data = request.get_json()
    status = data.get('status',None)


    dados = sta.StatusFac(status).cadastrarStatus()

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


@StatusFaccionostaEmProcesso_routes.route('/pcp/api/ExcluirStatus', methods=['DELETE'])
@token_required
def post_ExcluirStatus():
    data = request.get_json()
    status = data.get('status',None)


    dados = sta.StatusFac(status).excluirStatus()

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


@StatusFaccionostaEmProcesso_routes.route('/pcp/api/ApontarStatusOP', methods=['POST'])
@token_required
def postApontarStatusOP():
    data = request.get_json()
    statusTerceirizado = data.get('statusTerceirizado',None)
    numeroOP = data.get('numeroOP',None)
    usuario = data.get('usuario',None)
    justificativa = data.get('justificativa','')



    dados = staOP.StatusOpsEmProcesso(None, statusTerceirizado, numeroOP, usuario,
                 justificativa).post_apontarStatusOP()

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


@StatusFaccionostaEmProcesso_routes.route('/pcp/api/DashboardFaccTotal', methods=['POST'])
@token_required
def postDashboardFaccTotal():
    data = request.get_json()


    dados = staOP.StatusOpsEmProcesso().dashboardPecasFaccionista()

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