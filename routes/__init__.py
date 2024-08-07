from flask import Blueprint
# Crie um Blueprint para as rotas
routes_blueprint = Blueprint('routes', __name__)


from .Planejamento.plano_route import planoPCP_routes
from .MonitorPedidos.monitor_route import MonitorPedidos_routes
from .MonitorPedidos.AutomacaoPedidos_route import AtualizaPedidosSku_routes
from .MonitorPedidos.AutomacaoOps_route import AtualizaOP_routes
from .MonitorPedidos.monitorOP_routes import MonitorOp_routes
from .GestaoOPAberto.PainelGestaoOP_routes import PainelGestaoOP_routes
from .PainelFaturamento.PainelFaturamento import dashboardTVroute
from .PortalWeb.rotasPlataformaWeb import rotasPlataformaWeb
from .GestaoOPAberto.JustificativaOP import JustificativaOP_routes
from .MonitorPedidos.FiltrosEspeciaisMonitor import FiltrosEspeciaisMonitor_routes
from .GestaoOPAberto.FilaFases import FilaDasFases_routes
from .ControleGolas.controleGolas import controleGolas_routes
from .Planejamento.loteCsw_route import loteCsw_routes
from .Planejamento.acomp_meta_plano import MetasFases_routes
from .Planejamento.TipoNotaCSW import TipoNotaCsw_routes
from .Planejamento.SaldoPlanoAnterior import SaldoPlanoAnt_routes
from .Planejamento.cronograma import cronograma_routes
from .Faccionista.faccionista import faccionista_routes
from .Planejamento.Meta_faccionistas import MetasFacicionista_routes
from .Planejamento.LeadTime import LeadTime_routes

routes_blueprint.register_blueprint(planoPCP_routes)
routes_blueprint.register_blueprint(MonitorPedidos_routes)
routes_blueprint.register_blueprint(AtualizaPedidosSku_routes)
routes_blueprint.register_blueprint(AtualizaOP_routes)
routes_blueprint.register_blueprint(MonitorOp_routes)
routes_blueprint.register_blueprint(PainelGestaoOP_routes)
routes_blueprint.register_blueprint(dashboardTVroute)
routes_blueprint.register_blueprint(rotasPlataformaWeb)
routes_blueprint.register_blueprint(JustificativaOP_routes)
routes_blueprint.register_blueprint(FiltrosEspeciaisMonitor_routes)
routes_blueprint.register_blueprint(FilaDasFases_routes)
routes_blueprint.register_blueprint(controleGolas_routes)
routes_blueprint.register_blueprint(loteCsw_routes)
routes_blueprint.register_blueprint(MetasFases_routes)
routes_blueprint.register_blueprint(TipoNotaCsw_routes)
routes_blueprint.register_blueprint(SaldoPlanoAnt_routes)
routes_blueprint.register_blueprint(cronograma_routes)
routes_blueprint.register_blueprint(faccionista_routes)
routes_blueprint.register_blueprint(MetasFacicionista_routes)
routes_blueprint.register_blueprint(LeadTime_routes)
