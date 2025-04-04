from flask import Blueprint, jsonify, request
from functools import wraps
from models import FaccionistaClass, LeadTimeClass, TipoOPClass
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

    leadTime = LeadTimeClass.LeadTimeCalculator(dataIncio, dataFim, arrayTipoOP)
    dados = leadTime.leadTimeCategoria()

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

    tipoOP = TipoOPClass.TipoOP()
    dados = tipoOP.obterTodosTipos()

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


@LeadTime_routes.route('/pcp/api/filtroProdutivo', methods=['GET'])
@token_required
def get_tiposDeProducaoAgrupado():

    dados = TipoOPClass.TipoOP().tiposDeProducaoAgrupado()
    # Converte o DataFrame para uma lista de dicionários de forma eficiente
    OP_data = dados.to_dict('records')

    # Libera a memória ocupada pelo DataFrame, se necessário
    del dados
    gc.collect()

    # Retorna os dados em formato JSON
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
    congelado = data.get('congelado',False)

    # Instancia a classe e obtém os dados
    leadTime1 = LeadTimeClass.LeadTimeCalculator(dataInicio, dataFim, arrayTipoOP, arrayCategorias, congelado)
    dados = leadTime1.getLeadTimeFases()
    if congelado ==False:
        leadTime1.LimpezaBackpCongelamento(3)

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
    leadTime1 = LeadTimeClass.LeadTimeCalculator(dataInicio, dataFim, arrayTipoOP, arrayCategorias)
    faccionistas = FaccionistaClass.Faccionista()

    dados = leadTime1.getLeadTimeFaccionistas(faccionistas.consultarCategoriaMetaFaccionista_S())

    # Converte o DataFrame para uma lista de dicionários de forma eficiente
    OP_data = dados.to_dict('records')

    # Libera a memória ocupada pelo DataFrame, se necessário
    del dados
    gc.collect()

    # Retorna os dados em formato JSON
    return jsonify(OP_data)