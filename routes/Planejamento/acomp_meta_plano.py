import pandas as pd
from flask import Blueprint, jsonify, request
from functools import wraps
from models.Planejamento import plano, loteCsw, acomp_meta_plano
from models.GestaoOPAberto import realizadoFases
import datetime
import pytz
from models import ProducaoFases, MetaFases

MetasFases_routes = Blueprint('MetasFases_routes', __name__)


def dayAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.datetime.now(fuso_horario)
    day = agora.strftime('%Y-%m-%d')
    return day

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@MetasFases_routes.route('/pcp/api/MetasFases', methods=['POST'])
@token_required
def pOST_MetasFases():

    data = request.get_json()
    dia = dayAtual()
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')
    dataMovFaseIni = data.get('dataMovFaseIni', dia)
    dataMovFaseFim = data.get('dataMovFaseFim', dia)
    congelado = data.get('congelado', False)
    dataBackupMetas = data.get('dataBackupMetas', '2025-03-26')

    print(data)
    if congelado =='' or congelado == '-':
        congelado = False
    else:
        congelado = congelado

    meta = MetaFases.MetaFases(codigoPlano, '','',dataMovFaseIni,dataMovFaseFim,congelado,arrayCodLoteCsw, '1',dataBackupMetas)
    dados1 = meta.backupMetasAnteriores()

    dados = acomp_meta_plano.MetasFase(codigoPlano,arrayCodLoteCsw,dataMovFaseIni, dataMovFaseFim, congelado,dados1)

    #dados = pd.merge(dados,dados1,on='nomeFase',how='left')


    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@MetasFases_routes.route('/pcp/api/MetasFases2', methods=['POST'])
@token_required
def pOST_MetasFases2():

    data = request.get_json()
    dia = dayAtual()
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')
    dataMovFaseIni = data.get('dataMovFaseIni', dia)
    dataMovFaseFim = data.get('dataMovFaseFim', dia)
    congelado = data.get('congelado', False)
    dataBackupMetas = data.get('dataBackupMetas', dia)

    print(data)
    if congelado =='' or congelado == '-':
        congelado = False
    else:
        congelado = congelado

    meta = MetaFases.MetaFases(codigoPlano, '','',dataMovFaseIni,dataMovFaseFim,congelado,arrayCodLoteCsw, '1',dataBackupMetas)
    dados = meta.backupMetasAnteriores()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@MetasFases_routes.route('/pcp/api/filtroTiposOP', methods=['GET'])
@token_required
def get_filtroTiposOP():


    dataInicio = request.args.get('dataInicio')
    dataFinal = request.args.get('dataFinal')
    codEmpresa = request.args.get('codEmpresa','1')



    realizado = ProducaoFases.ProducaoFases(dataInicio, dataFinal, '','',codEmpresa)
    dados = realizado.lotesFiltragrem()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@MetasFases_routes.route('/pcp/api/MetasFasesCosturaCategorias', methods=['POST'])
@token_required
def pOST_MetasFasesCostura():

    data = request.get_json()
    dia = dayAtual()
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')
    dataMovFaseIni = data.get('dataMovFaseIni', dia)
    dataMovFaseFim = data.get('dataMovFaseFim', dia)
    congelado = data.get('congelado', False)
    print(f'Requisicao{data}')
    if congelado =='' or congelado == '-':
        congelado = False
    else:
        congelado = congelado


    dados = acomp_meta_plano.MetasCostura(codigoPlano,arrayCodLoteCsw,dataMovFaseIni, dataMovFaseFim, congelado)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@MetasFases_routes.route('/pcp/api/RealizadoGeralCostura', methods=['POST'])
@token_required
def pOST_RealizadoGeralCostura():

    data = request.get_json()
    dia = dayAtual()

    dataMovFaseIni = data.get('dataMovFaseIni', dia)
    dataMovFaseFim = data.get('dataMovFaseFim', dia)


    dados = realizadoFases.RealizadoFaseDia(dataMovFaseIni, dataMovFaseFim, 429)

    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@MetasFases_routes.route('/pcp/api/RealizadoFaseDiaFaccionista', methods=['POST'])
@token_required
def pOST_RealizadoFaseDiaFaccionista():

    data = request.get_json()
    dia = dayAtual()

    dataMovFaseIni = data.get('dataMovFaseIni', dia)
    dataMovFaseFim = data.get('dataMovFaseFim', dia)
    codFaccionista = data.get('codFaccionista', dia)


    dados = realizadoFases.RealizadoFaseDiaFaccionista(dataMovFaseIni, dataMovFaseFim, codFaccionista)

    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)




@MetasFases_routes.route('/pcp/api/RetornoPorFaseDiaria', methods=['GET'])
@token_required
def get_RetornoPorFaseDiaria():

    nomeFase = request.args.get('nomeFase')
    dataInicio = request.args.get('dataInicio')
    dataFinal = request.args.get('dataFinal')
    codEmpresa = request.args.get('codEmpresa','1')
    print(dataInicio)
    print(dataFinal)

    realizado = ProducaoFases.ProducaoFases(dataInicio, dataFinal, '','',codEmpresa,'','',[6, 8],'nao',nomeFase)
    dados = realizado.realizadoFasePeriodo()


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

