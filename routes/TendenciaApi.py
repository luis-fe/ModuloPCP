from flask import Blueprint, jsonify, request
from functools import wraps
from models import TendenciasPlano


tendencia_routes = Blueprint('tendencia_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



@tendencia_routes.route('/pcp/api/consultaParametrizacaoABC', methods=['GET'])
@token_required
def get_consultaParametrizacaoABC():

    dados = TendenciasPlano.TendenciaPlano().consultaParametrizacaoABC()
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


@tendencia_routes.route('/pcp/api/CadastrarParametroABC', methods=['POST'])
@token_required
def post_CadastrarParametroABC():
    data = request.get_json()

    parametroABC = data.get('parametroABC')


    dados = TendenciasPlano.TendenciaPlano('',parametroABC).inserirParametroABC()
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

@tendencia_routes.route('/pcp/api/consultaPlanejamentoABC_plano', methods=['GET'])
@token_required
def get_consultaPlanejamentoABC_plano():

    codPlano = request.args.get('codPlano','-')
    dados = TendenciasPlano.TendenciaPlano(codPlano).consultaPlanejamentoABC()
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


@tendencia_routes.route('/pcp/api/InserirOuAlterPlanoABC', methods=['POST'])
@token_required
def post_InserirOuAlterPlanoABC():
    data = request.get_json()

    codPlano = data.get('codPlano')
    nomeABC = data.get('nomeABC')
    perc_dist = data.get('perc_dist')


    dados = TendenciasPlano.TendenciaPlano(codPlano,nomeABC,perc_dist).inserirOuAlterarPlanj_ABC()
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



@tendencia_routes.route('/pcp/api/tendenciaSku', methods=['POST'])
@token_required
def post_tendenciaSku():
    data = request.get_json()

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')


    dados = TendenciasPlano.TendenciaPlano(codPlano,'','',empresa,consideraPedBloq).tendenciaVendas()
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

@tendencia_routes.route('/pcp/api/ABCReferencia', methods=['POST'])
@token_required
def post_ABCReferencia():
    data = request.get_json()

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')


    dados = TendenciasPlano.TendenciaPlano(codPlano,'','',empresa,consideraPedBloq).tendenciaAbc()
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


@tendencia_routes.route('/pcp/api/simulacaoProgramacao', methods=['POST'])
@token_required
def post_simulacaoProgramacao():
    data = request.get_json()

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    arraySimulaAbc = data.get('arraySimulaAbc')


    dados = TendenciasPlano.TendenciaPlano(codPlano,'','',empresa,consideraPedBloq).simulacaoProgramacao(arraySimulaAbc)
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