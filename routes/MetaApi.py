from flask import Blueprint, jsonify, request
from functools import wraps
from models import Meta, ProdutosClass, MetaFases

metas_routes = Blueprint('metas_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@metas_routes.route('/pcp/api/MetaGeralPlano', methods=['GET'])
@token_required
def get_MetaGeralPlano():
    codPlano = request.args.get('codPlano','-')

    dados = Meta.Meta(codPlano).consultaMetaGeral()
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

@metas_routes.route('/pcp/api/inserirOuAtualizarMetaPlano', methods=['POST'])
@token_required
def post_inserirOuAtualizarMetaPlano():
    data = request.get_json()

    codPlano = data.get('codPlano')
    marca = data.get('marca', '-')
    metaFinanceira = data.get('metaFinanceira', '-')
    metaPecas = data.get('metaPecas', '-')


    dados = Meta.Meta(codPlano,marca, metaFinanceira, metaPecas).inserirOuAtualizarMetasGeraisPlano()
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


@metas_routes.route('/pcp/api/MarcasDisponiveis', methods=['GET'])
@token_required
def get_MarcasDisponiveis():

    dados = ProdutosClass.Produto().consultaMarcasDisponiveis()
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

@metas_routes.route('/pcp/api/CategoriasDisponiveis', methods=['GET'])
@token_required
def get_CategoriasDisponiveis():

    dados = ProdutosClass.Produto().categoriasDisponiveis()
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


@metas_routes.route('/pcp/api/consultarMetaCategoriaPlano', methods=['GET'])
@token_required
def get_consultarMetaCategoriaPlano():
    codPlano = request.args.get('codPlano', '-')
    marca = request.args.get('marca', '-')

    dados = Meta.Meta(codPlano,marca).consultarMetaCategoriaPlano()
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


@metas_routes.route('/pcp/api/atualizaOuInserirMetaCategoria', methods=['POST'])
@token_required
def post_atualizaOuInserirMetaCategoria():
    data = request.get_json()

    codPlano = data.get('codPlano')
    marca = data.get('marca', '-')
    metaFinanceira = data.get('metaFinanceira', '-')
    metaPecas = data.get('metaPecas', '-')
    nomeCategoria = data.get('nomeCategoria', '-')

    dados = Meta.Meta(codPlano,marca, metaFinanceira, metaPecas, nomeCategoria).atualizaOuInserirMetaCategoria()
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




@metas_routes.route('/pcp/api/previsaoCategoriaFase', methods=['GET'])
@token_required
def get_previsaoCategoriaFase():
    nomeFase = request.args.get('nomeFase', '-')

    meta = MetaFases.MetaFases('','',nomeFase)

    dados = meta.previsao_categoria_fase()
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


@metas_routes.route('/pcp/api/faltaProgcategoria_fase', methods=['GET'])
@token_required
def get_faltaProgcategoria_fase():
    nomeFase = request.args.get('nomeFase', '-')

    meta = MetaFases.MetaFases('','',nomeFase)

    dados = meta.faltaProgcategoria_fase()
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


@metas_routes.route('/pcp/api/cargacategoria_fase', methods=['GET'])
@token_required
def get_cargacategoria_fase():
    nomeFase = request.args.get('nomeFase', '-')

    meta = MetaFases.MetaFases('','',nomeFase)

    dados = meta.cargaProgcategoria_fase()
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


@metas_routes.route('/pcp/api/Filacategoria_fase', methods=['GET'])
@token_required
def get_Filacategoria_fase():
    nomeFase = request.args.get('nomeFase', '-')

    meta = MetaFases.MetaFases('','',nomeFase)

    dados = meta.obterRoteirosFila()
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


@metas_routes.route('/pcp/api/FaltaProduzircategoria_fase', methods=['GET'])
@token_required
def get_FaltaProduzircategoria_fase():
    nomeFase = request.args.get('nomeFase', '-')
    codPlano = request.args.get('codPlano', '-')

    meta = MetaFases.MetaFases(codPlano,'',nomeFase)

    dados = meta.faltaProduzirCategoriaFase()
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