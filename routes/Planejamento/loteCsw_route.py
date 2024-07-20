from flask import Blueprint, jsonify, request
from functools import wraps
from models.Planejamento import plano, loteCsw

loteCsw_routes = Blueprint('loteCsw_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@loteCsw_routes.route('/pcp/api/lotes_csw', methods=['GET'])
@token_required
def get_lotes_csw():
    empresa = request.args.get('empresa','1')

    dados = loteCsw.lote(empresa)
    return jsonify(dados)
