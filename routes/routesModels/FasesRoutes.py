from flask import Blueprint, jsonify, request
from functools import wraps
from models import FaseClass
import gc
Fase_routes = Blueprint('Fase_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@Fase_routes.route('/pcp/api/ConsultaFasesProdutivaGestao', methods=['GET'])
@token_required
def get_ConsultaFasesProdutivaGestao():

    fasesProducao =FaseClass.FaseProducao()
    dados = fasesProducao.ConsultaFasesProdutivaCSWxGestao()

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

@Fase_routes.route('/pcp/api/InserirMetaResponsavel', methods=['PUT'])
@token_required
def put_InserirMetaResponsavel():

    data = request.get_json()
    codFase = data.get('codFase')
    responsavel = data.get('responsavel')
    leadTimeMeta = data.get('leadTimeMeta')


    fasesProducao =FaseClass.FaseProducao(codFase, responsavel, leadTimeMeta)
    dados = fasesProducao.AlterarMetaLT_Responsave()

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

@Fase_routes.route('/pcp/api/UpdateMetaResponsavel', methods=['POST'])
@token_required
def put_UpdateMetaResponsavell():

    data = request.get_json()
    codFase = data.get('codFase')
    responsavel = data.get('responsavel')
    leadTimeMeta = data.get('leadTimeMeta')


    fasesProducao =FaseClass.FaseProducao(codFase, responsavel, leadTimeMeta)
    dados = fasesProducao.AlterarMetaLT_Responsave()

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