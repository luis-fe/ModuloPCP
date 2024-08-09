from flask import Blueprint, jsonify, request
from functools import wraps
from models.GestaoOPAberto import realizadoFases

LeadTime_routes = Blueprint('LeadTime_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@LeadTime_routes.route('/pcp/api/LeadTimesRealizados', methods=['POST'])
@token_required
def get_LeadTimesRealizados():
    data = request.get_json()

    dataIncio = data.get('dataIncio')
    dataFim = data.get('dataFim')


    dados = realizadoFases.LeadTimeRealizado(dataIncio, dataFim)
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


@LeadTime_routes.route('/pcp/api/ObterTipoOP', methods=['GET'])
@token_required
def get_ObterTipoOP():

    dados = realizadoFases.ObterTipoOPs()

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