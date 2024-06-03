import pandas as pd
from flask import Flask, jsonify
import os
from models import loteCsw

app = Flask(__name__)
port = int(os.environ.get('PORT',5000))

@app.route("/api/teste", methods=['GET'])
def teste():

    x = loteCsw.lote(1)
    # Obtém os nomes das colunas
    column_names = x.columns
    OP_data = []
    for index, row in x.iterrows():
            op_dict = {}
            for column_name in column_names:
                op_dict[column_name] = row[column_name]
            OP_data.append(op_dict)

    return jsonify(OP_data)
    

if __name__ == '__main__':
    
    app.run(host='0.0.0.0', port=port)
