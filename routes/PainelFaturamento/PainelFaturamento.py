from flask import Blueprint,Flask, render_template, jsonify, request
from functools import wraps
from flask_cors import CORS
import pandas as pd
from models.PainelFaturamento import PainelFaturamento
import subprocess

dashboardTVroute = Blueprint('dashboardTVroute', __name__)
CORS(dashboardTVroute)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@dashboardTVroute.route('/pcp/api/dashboarTV', methods=['GET'])
def dashboarTV():
    try:
        ano = request.args.get('ano', '2024')
        empresa = request.args.get('empresa', 'Todas')

        if empresa == 'Outras':
            usuarios = PainelFaturamento.OutrosFat(ano, empresa)
            usuarios = pd.DataFrame(usuarios)
        else:
            usuarios = PainelFaturamento.Faturamento_ano(ano, empresa)
            usuarios = pd.DataFrame(usuarios)

        # Obtém os nomes das colunas
        column_names = usuarios.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        OP_data = []
        for index, row in usuarios.iterrows():
            op_dict = {}
            for column_name in column_names:
                op_dict[column_name] = row[column_name]
            OP_data.append(op_dict)

        return jsonify(OP_data)
    except Exception as e:
        print(f"Erro detectado: {str(e)}")
        restart_server()
        return jsonify({"error": "O servidor foi reiniciado devido a um erro."})


def restart_server():
    print("Reiniciando o aplicativo...")
    subprocess.call(["python", "main.py"])