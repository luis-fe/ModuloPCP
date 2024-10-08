from flask import Flask, jsonify
import os
from dotenv import load_dotenv
from routes import routes_blueprint

app = Flask(__name__)
port = int(os.environ.get('PORT',8000))
load_dotenv('config/db.env')

app.register_blueprint(routes_blueprint)

if __name__ == '__main__':
    api_key = os.getenv('caminho')

    print(api_key)
    app.run(host='0.0.0.0', port=port)
