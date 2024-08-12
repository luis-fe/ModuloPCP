from flask import Blueprint, jsonify, request
from functools import wraps
from models.NecessidadesDeMP import necessidadeMP
import datetime
import pytz

NecessidadesMP_routes = Blueprint('NecessidadesMP_routes', __name__)


def dayAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    day = agora.strftime('%Y-%m-%d')
    return day

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@NecessidadesMP_routes.route('/pcp/api/AnaliseDeMateriais', methods=['POST'])
@token_required
def pOST_AnaliseDeMateriais():

    data = request.get_json()
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')
    congelado = data.get('congelado', False)

    if congelado =='' or congelado == '-':
        congelado = False
    else:
        congelado = congelado


    dados = necessidadeMP.AnaliseDeMateriais(codigoPlano,arrayCodLoteCsw, congelado)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

