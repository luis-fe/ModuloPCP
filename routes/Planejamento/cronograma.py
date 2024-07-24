from flask import Blueprint, jsonify, request
from functools import wraps
from models.Planejamento import plano, cronograma

cronograma_routes = Blueprint('cronograma_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@cronograma_routes.route('/pcp/api/ConsultaCronogramaFasePlano', methods=['GET'])
@token_required
def get_ConsultaCronogramaFasePlano():
    codigoPlano = request.args.get('codigoPlano')


    dados = cronograma.ConsultarCronogramaFasesPlano(codigoPlano)
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


@cronograma_routes.route('/pcp/api/ConsultaCronogramaFasePlanoFase', methods=['GET'])
@token_required
def get_ConsultaCronogramaFasePlanoFase():
    codigoPlano = request.args.get('codigoPlano')
    codFase = request.args.get('codFase')

    dados = cronograma.ConsultarCronogramaFasesPlano(codigoPlano)
    dados = dados[dados['codFase']==str(codFase)]
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



@cronograma_routes.route('/pcp/api/NovoIntervaloFasePla', methods=['POST'])
@token_required
def pOST_NovoIntervaloFasePla():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    codFase = data.get('codFase')
    dataInicio = data.get('dataInicio')
    dataFim = data.get('dataFim')


    dados = cronograma.InserirIntervaloFase(codigoPlano, codFase, dataInicio, dataFim)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)