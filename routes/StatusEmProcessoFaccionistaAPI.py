'''
CONTROLE DAS APIS REQUISITADAS QUE INTERAGEM COM AS CLASSES :
StatusOPsEmProcesso , PRESENTE NO DIRETORIO MODELS
'''

from flask import Blueprint, jsonify, request
from functools import wraps
from models import StatusOpsEmProcesso as sta

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
    categoria = data.get('categoria')
    faccionista = data.get('faccionista', None)

    dados = sta.StatusOpsEmProcesso(None,None,None,None,None,None,None,categoria).getOPsEmProcessoCategoria()

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