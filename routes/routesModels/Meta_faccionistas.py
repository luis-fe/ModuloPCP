from flask import Blueprint, jsonify, request
from functools import wraps
from models.Planejamento import plano, loteCsw, Meta_faccionistas
from models import MetaFaccionistaClass
import datetime
import pytz

MetasFacicionista_routes = Blueprint('MetasFacicionista_routes', __name__)


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

@MetasFacicionista_routes.route('/pcp/api/MetasFaccionista', methods=['POST'])
@token_required
def pOST_MetasFaccionista():

    data = request.get_json()
    dia = dayAtual()
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')
    dataMovFaseIni = data.get('dataMovFaseIni', dia)
    dataMovFaseFim = data.get('dataMovFaseFim', dia)
    congelado = data.get('congelado', False)


    if congelado =='' or congelado == '-':
        congelado = False
    else:
        congelado = congelado

    metas = MetaFaccionistaClass.MetaFaccionista(codigoPlano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado)
    dados = metas.getMetaFaccionista()
    column_names = dados.columns

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)