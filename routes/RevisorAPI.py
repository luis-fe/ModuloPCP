from flask import Blueprint, jsonify, request
from functools import wraps
from models import Revisor as r


Revisores_Routes = Blueprint('Revisores_Routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@Revisores_Routes.route('/pcp/api/consultaRevisores', methods=['GET'])
@token_required
def get_consultaRevisores():
    empresa = request.args.get('empresa','1')

    revisor = r.Revisor('','',empresa)
    dados = revisor.consultaRevisores()
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


@Revisores_Routes.route('/pcp/api/PesquisarRevisorEspecifico', methods=['GET'])
@token_required
def get_PesquisarRevisorEspecifico():
    empresa = request.args.get('empresa','1')
    codRevisor = request.args.get('codRevisor','1')

    revisor = r.Revisor(codRevisor,'',empresa)
    dados = revisor.pesquisarRevisorEspecifico()
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

@Revisores_Routes.route('/pcp/api/InativarRevisor', methods=['PUT'])
@token_required
def get_cInativarRevisor():
    datas = request.get_json()

    codRevisor = datas['codRevisor']
    empresa = datas['empresa']
    situacaoRevisor = datas['situacaoRevisor']


    revisor = r.Revisor(codRevisor,'',empresa,situacaoRevisor)
    dados = revisor.inativarRevisor()

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


@Revisores_Routes.route('/pcp/api/cadastrarRevisor', methods=['POST'])
@token_required
def post_cadastrarRevisor():
    datas = request.get_json()

    codRevisor = datas['codRevisor']
    empresa = datas['empresa']
    nomeRevisor = datas['nomeRevisor']


    revisor = r.Revisor(codRevisor,nomeRevisor,empresa,'ATIVO')
    dados = revisor.cadastrarRevisor()

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


@Revisores_Routes.route('/pcp/api/ExcluirRevisor', methods=['DELETE'])
@token_required
def post_ExcluirRevisor():
    datas = request.get_json()

    codRevisor = datas['codRevisor']
    empresa = datas['empresa']


    revisor = r.Revisor(codRevisor,'',empresa,'ATIVO')
    dados = revisor.exlcuirRevisor()

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



@Revisores_Routes.route('/pcp/api/AlterarRevisor', methods=['PUT'])
@token_required
def post_AlterarRevisor():
    datas = request.get_json()

    codRevisor = datas['codRevisor']
    nomeRevisor = datas['nomeRevisor']
    empresa = datas.get('empresa','1')
    situacaoRevisor = datas.get('situacaoRevisor', 'ATIVO')  # Define 'ATIVO' como padrão


    revisor = r.Revisor(codRevisor,nomeRevisor,empresa,situacaoRevisor)
    dados = revisor.alterarRevisor()

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