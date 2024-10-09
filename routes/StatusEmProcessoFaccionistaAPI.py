'''
CONTROLE DAS APIS REQUISITADAS QUE INTERAGEM COM AS CLASSES :
StatusOPsEmProcesso , PRESENTE NO DIRETORIO MODELS
'''

from flask import Blueprint, jsonify, request
from functools import wraps
from models import Faccionista as fac

StatusFaccionostaEmProcesso_routes = Blueprint('StatusFaccionostaEmProcesso_routes', __name__)