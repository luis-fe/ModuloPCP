from flask import Blueprint, jsonify, request, Response
from functools import wraps
from models import AnaliseMateriais
import io


analiseMP_routes = Blueprint('analiseMP_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@analiseMP_routes.route('/pcp/api/AnaliseMateriaisPelaTendencia', methods=['POST'])
@token_required
def post_AnaliseMateriaisPelaTendencia():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')


    dados = AnaliseMateriais.AnaliseMateriais(codPlano, consideraPedBloq).estruturaItens('nao')
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

@analiseMP_routes.route('/pcp/api/SimulaAnaliseMateriaisPelaTendencia', methods=['POST'])
@token_required
def post_SimulaAnaliseMateriaisPelaTendenciaa():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    arraySimulaAbc = data.get('arraySimulaAbc')



    dados = AnaliseMateriais.AnaliseMateriais(codPlano, consideraPedBloq).estruturaItens('nao',arraySimulaAbc)
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



@analiseMP_routes.route('/pcp/api/DetalhaNecessidade', methods=['POST'])
@token_required
def post_DetalhaNecessidade():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codComponente = data.get('codComponente')



    dados = AnaliseMateriais.AnaliseMateriais(codPlano, '',consideraPedBloq,codComponente).detalhaNecessidade()
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


@analiseMP_routes.route('/pcp/api/consultaImagemItem', methods=['GET'])
@token_required
def ger_consultaImagemItem():



    imagem_data = AnaliseMateriais.AnaliseMateriais().consultaImagem()
    if imagem_data:
        # Retorna a imagem no formato JPEG
        return Response(
            io.BytesIO(imagem_data),
            mimetype='image/jpeg',
            headers={"Content-Disposition": "inline; filename=imagem.jpeg"}
        )
    else:
        # Retorna uma mensagem de erro se não houver dados
        return jsonify({"error": "Imagem não encontrada"}), 404



@analiseMP_routes.route('/pcp/api/naturezaEstoqueComponentes', methods=['GET'])
@token_required
def ger_naturezaEstoqueComponentes():



    dados = AnaliseMateriais.AnaliseMateriais().sqlEstoqueItem()
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