from flask import Blueprint
# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)


from .Planejamento.plano_route import planoPCP_routes
from .MonitorPedidos.monitor_route import MonitorPedidos_routes

routes_blueprint.register_blueprint(planoPCP_routes)
routes_blueprint.register_blueprint(MonitorPedidos_routes)
