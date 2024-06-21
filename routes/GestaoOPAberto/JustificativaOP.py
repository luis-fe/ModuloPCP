'''
Modelagem do painel de controle das Ops na fabrica
'''

from models.ControleApi import controle
from flask import Blueprint, jsonify, request
from functools import wraps
from flask_cors import CORS
from models.GestaoOPAberto import justificativasOP

JustificativaOP_routes = Blueprint('JustificativaOP', __name__)
CORS(JustificativaOP_routes)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



@JustificativaOP_routes.route('/pcp/api/ConsultarJustificativa', methods=['GET'])
@token_required
def ConsultarJustificativa():

    ordemProd = request.args.get('ordemProd')
    fase = request.args.get('fase', '-')

    plano = justificativasOP.ConsultarJustificativa(ordemProd, fase)
    column_names = plano.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in plano.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@JustificativaOP_routes.route('/pcp/api/CadastrarJustificativa', methods=['PUT'])
@token_required
def CadastrarJustificativa():

    data = request.get_json()

    ordemProd = data.get('ordemProd', '-')
    fase = data.get('fase', '-')
    justificativa = data.get('justificativa', '-')

    plano = justificativasOP.CadastrarJustificativa(ordemProd, fase, justificativa)
    column_names = plano.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in plano.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)