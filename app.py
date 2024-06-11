import pandas as pd
from flask import Flask, jsonify
import os
from models import loteCsw
import gc
from routes import routes_blueprint

app = Flask(__name__)
port = int(os.environ.get('PORT',5001))

app.register_blueprint(routes_blueprint)


@app.route("/api/teste", methods=['GET'])
def teste():
    x = loteCsw.lote(1)
    
    # Otimiza a conversão para JSON
    OP_data = x.to_dict(orient='records')
    
    # Libera memória não utilizada
    del x
    gc.collect()

    return jsonify(OP_data)
    

if __name__ == '__main__':
    
    app.run(host='0.0.0.0', port=port)
