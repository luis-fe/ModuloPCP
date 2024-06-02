import pandas as pd
from flask import Flask
import os
from models import loteCsw

app = Flask(__name__)
port = int(os.environ.get('PORT',5000))

x = loteCsw.lote(1)
print(x)

if __name__ == '__main__':
    
    app.run(host='0.0.0.0', port=port)
