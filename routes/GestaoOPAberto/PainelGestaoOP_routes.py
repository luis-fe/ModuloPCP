'''
Modelagem do painel de controle das Ops na fabrica
'''

from models.ControleApi import controle
from flask import Blueprint, jsonify, request
from functools import wraps
from flask_cors import CORS
from models.GestaoOPAberto import PainelGestaoOP

PainelGestaoOP_routes = Blueprint('PainelGestaoOP', __name__)
CORS(PainelGestaoOP_routes)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@PainelGestaoOP_routes.route('/pcp/api/CargaOPs', methods=['POST'])
@token_required
def CargadasOPs():


    data = request.get_json()
    empresa = data.get('empresa','1')
    filtro = data.get('filtro', '-')
    area = data.get('area', 'PRODUCAO')
    filtroDiferente = data.get('filtroDiferente', '')
    rotina = 'Portal Consulta OP'
    classificar = data.get('classificar', '-')
    colecao = data.get('colecao','')
    PainelGestaoOP.ExcluindoDuplicatasJustificativas()

    if colecao == []:
        colecao = ''

    print(f'foi classficado por {classificar}')
    client_ip = request.remote_addr



    datainicio = controle.obterHoraAtual()
    tempo = controle.TempoUltimaAtualizacaoPCP(datainicio,rotina)
    limite = 60
    limiteFiltros = 600

    if (filtro == '-' and filtroDiferente == '' and tempo >= limite  ) or (filtro == '' and filtroDiferente == '' and tempo >= limite)  :
        usuarios = PainelGestaoOP.OPemProcesso(empresa, area, filtro, filtroDiferente, tempo, limite,classificar,colecao)  ## Aqui defino que o tempo limite de requisicao no csw é acima de 60 segundos, evitando a simultanedade de requisicao
        controle.salvar('Portal Consulta OP',client_ip,datainicio)
        controle.ExcluirHistorico(3)

    elif tempo >= limiteFiltros :
        usuarios1 = PainelGestaoOP.OPemProcesso(empresa, area, '-', '', tempo, limite,classificar,colecao)  ## Aqui defino que o tempo limite de requisicao no csw é acima de 60 segundos, evitando a simultanedade de requisicao

        controle.salvar('Portal Consulta OP',client_ip,datainicio)
        controle.ExcluirHistorico(3)
        usuarios = PainelGestaoOP.OPemProcesso(empresa, area, filtro, filtroDiferente, tempo, limite,classificar,colecao)  ## Aqui defino que o tempo limite de requisicao no csw é acima de 60 segundos, evitando a simultanedade de requisicao
        print('Resultado')
        print(usuarios)

    else:
        usuarios = PainelGestaoOP.OPemProcesso(empresa, area, filtro, filtroDiferente, tempo, limite,classificar,colecao)  ## Aqui defino que o tempo limite de requisicao no csw é acima de 60 segundos, evitando a simultanedade de requisicao
        print(client_ip+' '+filtro)
        print('Resultado')
        print(usuarios)

        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []

    for index, row in usuarios.iterrows():
        op_dict = {column_name: row[column_name] for column_name in usuarios.columns}

        # Se "3 - Detalhamento" existir e for uma lista de dicionários, reordena as colunas no detalhamento
        if "3 -Detalhamento" in op_dict and isinstance(op_dict["3 -Detalhamento"], list):
            detalhamento = []
            for detail in op_dict["3 -Detalhamento"]:
                ordered_detail = {
                    "numeroOP": detail.get("numeroOP"),
                    "codProduto": detail.get("codProduto"),
                    **{k: v for k, v in detail.items() if k not in ["numeroOP", "codProduto"]}
                }
                detalhamento.append(ordered_detail)
            op_dict["3 -Detalhamento"] = detalhamento

        OP_data.append(op_dict)

    return jsonify(OP_data), 200
@PainelGestaoOP_routes.route('/pcp/api/DistinctColecao', methods=['GET'])
@token_required
def DistinctColecao():


    usuarios = PainelGestaoOP.DistinctColecao()


    # Obtém os nomes das colunas
    column_names = usuarios.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in usuarios.iterrows():
        op_dict = {}
        for index, row in usuarios.iterrows():
            op_dict = {}
            for column_name in column_names:
                op_dict[column_name] = row[column_name]
            OP_data.append(op_dict)
        return jsonify(OP_data)