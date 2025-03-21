from flask import Flask, jsonify
import os
from dotenv import load_dotenv, dotenv_values
from routes import routes_blueprint

app = Flask(__name__)
port = int(os.environ.get('PORT', 8000))

# Carregar o arquivo de variáveis de ambiente no caminho correto
load_dotenv('/home/mplti/ModuloPCP/db.env')

app.register_blueprint(routes_blueprint)

if __name__ == '__main__':
    # Certifique-se de usar o nome correto da variável
    api_key = os.getenv('CAMINHO')  # Troque por 'API_KEY' ou outro nome se necessário

    print(api_key)  # Exibe o valor da API Key
    print(f"API Key: {api_key}")  # Deve exibir o valor de API_KEY do db.env
    config = dotenv_values('db.env')
    print(config)  # Isso deve imprimir o conteúdo do .env como um dicionário

    app.run(host='0.0.0.0', port=port)
