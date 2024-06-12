from flask import Blueprint, jsonify, request
from functools import wraps
from models.MonitorPedidos import monitor

MonitorPedidos_routes = Blueprint('MonitorPedidos_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@MonitorPedidos_routes.route('/pcp/api/monitorPreFaturamento', methods=['GET'])
@token_required
def get_MonitorPedidos():
    empresa = request.args.get('empresa')
    iniVenda = request.args.get('iniVenda','-')
    finalVenda = request.args.get('finalVenda')
    tiponota = request.args.get('tiponota')
    parametroClassificacao = request.args.get('parametroClassificacao', 'DataPrevisao')  # Faturamento ou DataPrevisao
    tipoData = request.args.get('tipoData','DataEmissao') #DataEmissao x DataPrevOri
    arrayRepres_excluir = request.args.get('arrayRepres_excluir','')
    arrayRepre_Incluir = request.args.get('arrayRepre_Incluir','')

    #controle.InserindoStatus(rotina, ip, datainicio)
    dados = monitor.API(empresa, iniVenda, finalVenda, tiponota,'rotina', 'ip', 'datainicio',parametroClassificacao, tipoData, arrayRepres_excluir, arrayRepre_Incluir)
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
    return jsonify(OP_data)