from flask import Blueprint, jsonify, request
from functools import wraps
from models.Planejamento import plano, loteCsw, acomp_meta_plano
from models.GestaoOPAberto import realizadoFases

MetasFases_routes = Blueprint('MetasFases_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@MetasFases_routes.route('/pcp/api/MetasFases', methods=['POST'])
@token_required
def pOST_MetasFases():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')
    dataMovFaseIni = data.get('dataMovFaseIni', '2024-07-25')
    dataMovFaseFim = data.get('dataMovFaseFim', '2024-07-25')



    dados = acomp_meta_plano.MetasFase(codigoPlano,arrayCodLoteCsw,dataMovFaseIni, dataMovFaseFim)
    realizadoFases.CarregarRealizado(60)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)