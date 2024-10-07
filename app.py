import pandas as pd
from flask import Flask, jsonify
import os
from models.Planejamento import loteCsw
import gc
from routes import routes_blueprint

app = Flask(__name__)
port = int(os.environ.get('PORT',8000))

app.register_blueprint(routes_blueprint)

if __name__ == '__main__':
    print('teste')
    app.run(host='0.0.0.0', port=port)
