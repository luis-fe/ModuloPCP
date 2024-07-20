from flask import Blueprint, jsonify, request
from functools import wraps
from models.Planejamento import plano

planoPCP_routes = Blueprint('planoPCP_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@planoPCP_routes.route('/pcp/api/Plano', methods=['GET'])
@token_required
def get_Plano():
    dados = plano.ObeterPlanos()
    return jsonify(dados)
