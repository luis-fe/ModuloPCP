from flask import Blueprint, jsonify, request
from functools import wraps
from models.Planejamento import plano
from models import PlanoClass

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
    dados = PlanoClass.Plano().obterPlanos()
    return jsonify(dados)

@planoPCP_routes.route('/pcp/api/PlanoPorPlano', methods=['GET'])
@token_required
def get_PlanoPorPlano():
    codigoPlano = request.args.get('codigoPlano')

    dados = PlanoClass.Plano(codigoPlano).obterPlanosPlano()
    return jsonify(dados)

@planoPCP_routes.route('/pcp/api/NovoPlano', methods=['POST'])
@token_required
def pOST_novoPlano():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    descricaoPlano = data.get('descricaoPlano', '-')
    iniVendas = data.get('iniVendas', '-')
    fimVendas = data.get('fimVendas', '-')
    iniFat = data.get('iniFat', '-')
    fimFat = data.get('fimFat', '-')
    usuarioGerador = data.get('usuarioGerador', '-')


    dados = PlanoClass.Plano(codigoPlano, descricaoPlano, iniVendas, fimVendas, iniFat, fimFat, usuarioGerador).inserirNovoPlano()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@planoPCP_routes.route('/pcp/api/AlterPlano', methods=['PUT'])
@token_required
def PUT_AlterPlano():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    descricaoPlano = data.get('descricaoPlano', '-')
    iniVendas = data.get('iniVendas', '-')
    fimVendas = data.get('fimVendas', '-')
    iniFat = data.get('iniFat', '-')
    fimFat = data.get('fimFat', '-')


    dados = PlanoClass.Plano(codigoPlano, descricaoPlano, iniVendas, fimVendas, iniFat, fimFat).AlterPlano()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@planoPCP_routes.route('/pcp/api/VincularLotesPlano', methods=['PUT'])
@token_required
def put_VincularLotesPlano():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')


    dados = PlanoClass.Plano(codigoPlano).vinculandoLotesAoPlano(arrayCodLoteCsw)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@planoPCP_routes.route('/pcp/api/VincularNotasPlano', methods=['PUT'])
@token_required
def put_VincularNotasPlano():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    arrayTipoNotas = data.get('arrayTipoNotas', '-')
    print(data)


    dados = plano.VincularNotasAoPlano(codigoPlano,arrayTipoNotas)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@planoPCP_routes.route('/pcp/api/DesvincularLotesPlano', methods=['DELETE'])
@token_required
def Delete_DesvincularLotesPlano():

    data = request.get_json()
    print(data)

    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')


    dados = PlanoClass.Plano(codigoPlano).desvincularLotesAoPlano(arrayCodLoteCsw)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@planoPCP_routes.route('/pcp/api/DesvincularNotasPlano', methods=['DELETE'])
@token_required
def Delete_DesvincularNotasPlano():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    arrayTipoNotas = data.get('arrayTipoNotas', '-')


    dados = plano.DesvincularNotasAoPlano(codigoPlano,arrayTipoNotas)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)




@planoPCP_routes.route('/pcp/api/ConsultaTipoNotasVinculados', methods=['GET'])
@token_required
def GET_ConsultaTipoNotasVinculados():
    planoParametro = request.args.get('plano', '-')
    dados = plano.ConsultarTipoNotasVinculados(str(planoParametro))
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)