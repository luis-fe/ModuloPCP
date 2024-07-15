'''
Modelagem do painel de fila das Fases
'''

from models.ControleApi import controle
from flask import Blueprint, jsonify, request
from functools import wraps
from flask_cors import CORS
from models.GestaoOPAberto import FilaFases

FilaDasFases_routes = Blueprint('FilaDasFases', __name__)
CORS(FilaDasFases_routes)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@FilaDasFases_routes.route('/pcp/api/FilaFases', methods=['POST'])
@token_required
def get_FilaDasFases_routes():
    #try:
        data = request.get_json()

        colecao = data.get('Colecao', '-')
        usuarios = FilaFases.ApresentacaoFila(colecao)

        # Obtém os nomes das colunas
        column_names = usuarios.columns

        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        OP_data = []
        for index, row in usuarios.iterrows():
            op_dict = {}
            for column_name in column_names:
                op_dict[column_name] = row[column_name]
            OP_data.append(op_dict)

        return jsonify(OP_data)

    #except Exception as e:
       # return jsonify({'error': str(e)}), 500

@FilaDasFases_routes.route('/pcp/api/DetalhaOpFilas', methods=['POST'])
@token_required
def get_DetalhaOpFilas():
    data = request.get_json()
    nomeFase = data.get('nomeFase', '-')

    usuarios = FilaFases.FiltrosFila(nomeFase)


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