'''
Modelagem do painel de fila das Fases
'''

from models.ControleApi import controle
from flask import Blueprint, jsonify, request
from functools import wraps
from flask_cors import CORS
from models.ControleGolas import controleGolas

controleGolas_routes = Blueprint('controleGolas', __name__)
CORS(controleGolas_routes)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@controleGolas_routes.route('/pcp/api/controleGolas', methods=['GET'])
@token_required
def get_controleGolas():


    usuarios = controleGolas.ControleGolasPunhos()


    # Obtém os nomes das colunas
    column_names = usuarios.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in usuarios.iterrows():
        op_dict = {}
        for index, row in usuarios.iterrows():
            op_dict = {}
            for column_name in column_names:
                op_dict[column_name] = row[column_name]
            OP_data.append(op_dict)
        return jsonify(OP_data)
