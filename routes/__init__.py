from flask import Blueprint
# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)


from .Planejamento.plano_route import planoPCP_routes
from .MonitorPedidos.monitor_route import MonitorPedidos_routes
from .MonitorPedidos.AutomacaoPedidos_route import AtualizaPedidosSku_routes
from .MonitorPedidos.AutomacaoOps_route import AtualizaOP_routes
from .MonitorPedidos.monitorOP_routes import MonitorOp_routes

routes_blueprint.register_blueprint(planoPCP_routes)
routes_blueprint.register_blueprint(MonitorPedidos_routes)
routes_blueprint.register_blueprint(AtualizaPedidosSku_routes)
routes_blueprint.register_blueprint(AtualizaOP_routes)
routes_blueprint.register_blueprint(MonitorOp_routes)
