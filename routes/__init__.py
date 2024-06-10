from flask import Blueprint
# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)


from plano_route import plano_routes


routes_blueprint.register_blueprint(plano_routes)
