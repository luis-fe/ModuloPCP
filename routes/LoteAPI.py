from flask import Blueprint, jsonify, request
from functools import wraps
from models import Lote as l


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

    lote = l.Lote(None,empresa)
    dados = lote.obterLotesCsw()
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

@loteCsw_routes.route('/pcp/api/ConsultaLotesVinculados', methods=['GET'])
@token_required
def GET_ConsultaLotesVinculados():
    planoParametro = request.args.get('plano', '-')
    dados = l.Lote(None,None,planoParametro).obterLotesporPlano()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)
