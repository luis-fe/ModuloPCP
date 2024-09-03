from flask import Blueprint, jsonify, request
from functools import wraps
from models.Faccionistas import faccionistas
from models import FaccionistaClass

faccionista_routes = Blueprint('faccionista_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@faccionista_routes.route('/pcp/api/ConsultaFaccionistasCsw', methods=['GET'])
@token_required
def get_ConsultaFaccionistasCsws():

    dados = faccionistas.ListaFaccionistasCsw()

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

@faccionista_routes.route('/pcp/api/ObterCategorias', methods=['GET'])
@token_required
def get_ObterCategorias():

    dados = faccionistas.ObterCategorias()

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

@faccionista_routes.route('/pcp/api/RegistroFaccionistas', methods=['GET'])
@token_required
def get_RegistroFaccionistas():

    faccionista = FaccionistaClass.Faccionista()
    dados = faccionista.RegistroFaccionistas()

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



@faccionista_routes.route('/pcp/api/CadastrarCapacidadeDiariaFac', methods=['POST'])
@token_required
def post_CadastrarCapacidadeDiariaFac():

    data = request.get_json()

    codFaccionista = data.get('codFaccionista')
    apelido = data.get('apelido', '-')
    ArrayCategorias = data.get('ArrayCategorias')
    ArrayCapacidade = data.get('ArrayCapacidade')

    dados =None
    #dados = faccionistas.CadastrarCapacidadeDiariaFac(codFaccionista,apelido,ArrayCategorias, ArrayCapacidade)
    for categoria, capacidade in zip(ArrayCategorias, ArrayCapacidade):
        Novofaccionista = FaccionistaClass.Faccionista(codFaccionista, apelido, categoria, capacidade)
        dados = Novofaccionista.InserirFaccionista()

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


