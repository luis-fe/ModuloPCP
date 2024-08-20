from flask import Blueprint, jsonify, request
from functools import wraps
from models.GestaoOPAberto import realizadoFases
from models.GestaoProducao import leadTimeClass, faccionistaClass
import gc
LeadTime_routes = Blueprint('LeadTime_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@LeadTime_routes.route('/pcp/api/LeadTimesRealizados', methods=['POST'])
@token_required
def get_LeadTimesRealizados():
    data = request.get_json()

    dataIncio = data.get('dataIncio')
    dataFim = data.get('dataFim')
    arrayTipoOP = data.get('arrayTipoOP',[])


    dados = realizadoFases.LeadTimeRealizado(dataIncio, dataFim,arrayTipoOP)

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


@LeadTime_routes.route('/pcp/api/ObterTipoOP', methods=['GET'])
@token_required
def get_ObterTipoOP():

    dados = realizadoFases.ObterTipoOPs()

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
@LeadTime_routes.route('/pcp/api/LeadTimesFases', methods=['POST'])
@token_required
def get_LeadTimesFases():
    data = request.get_json()

    # Corrigindo o nome da variável para 'dataInicio'
    dataInicio = data.get('dataInicio')
    dataFim = data.get('dataFim')
    arrayTipoOP = data.get('arrayTipoOP', [])
    arrayCategorias = data.get('arrayCategorias', [])

    # Instancia a classe e obtém os dados
    leadTime1 = leadTimeClass.LeadTimeCalculator(dataInicio, dataFim,arrayTipoOP,arrayCategorias)
    dados = leadTime1.getLeadTimeFases()

    # Converte o DataFrame para uma lista de dicionários de forma eficiente
    OP_data = dados.to_dict('records')

    # Libera a memória ocupada pelo DataFrame, se necessário
    del dados
    gc.collect()

    # Retorna os dados em formato JSON
    return jsonify(OP_data)

@LeadTime_routes.route('/pcp/api/LeadTimesFaccionistas', methods=['POST'])
@token_required
def get_LeadTimesFaccionistas():
    data = request.get_json()

    # Corrigindo o nome da variável para 'dataInicio'
    dataInicio = data.get('dataInicio')
    dataFim = data.get('dataFim')
    arrayTipoOP = data.get('arrayTipoOP', [])
    arrayCategorias = data.get('arrayCategorias', [])

    # Instancia a classe e obtém os dados
    leadTime1 = leadTimeClass.LeadTimeCalculator(dataInicio, dataFim,arrayTipoOP,arrayCategorias)
    faccionista = faccionistaClass.Faccionista()

    dados = leadTime1.getLeadTimeFaccionistas(faccionista.ConsultarFaccionista())

    # Converte o DataFrame para uma lista de dicionários de forma eficiente
    OP_data = dados.to_dict('records')

    # Libera a memória ocupada pelo DataFrame, se necessário
    del dados
    gc.collect()

    # Retorna os dados em formato JSON
    return jsonify(OP_data)