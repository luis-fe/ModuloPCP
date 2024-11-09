from flask import Blueprint, jsonify, request
from functools import wraps
from models import LiberacaoQualidade as lib


LiberacaoQualidade_Routes = Blueprint('LiberacaoQualidade', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@LiberacaoQualidade_Routes.route('/pcp/api/consultarCargaCarrinho', methods=['GET'])
@token_required
def get_consultarCargaCarrinho():
    empresa = request.args.get('empresa','1')
    Ncarrinho = request.args.get('Ncarrinho','-')

    carrinho = lib.Liberacao(Ncarrinho,'',empresa)
    dados = carrinho.consultarCargaCarrinho()
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

@LiberacaoQualidade_Routes.route('/pcp/api/atribuirOPRevisorArray', methods=['POST'])
@token_required
def post_atribuirOPRevisorArray():
    datas = request.get_json()
    array = datas['array']


    carrinho = lib.Liberacao('','','')
    dados = carrinho.atribuirOPRevisorArray(array)

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


@LiberacaoQualidade_Routes.route('/pcp/api/produtividadeRevisoresPeriodo', methods=['GET'])
@token_required
def get_produtividadeRevisoresPeriodo():
    dataInicio = request.args.get('dataInicio','1')
    dataFinal = request.args.get('dataFinal','-')

    carrinho = lib.Liberacao('','','')
    dados = carrinho.produtividadePeriodo(dataInicio, dataFinal)
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