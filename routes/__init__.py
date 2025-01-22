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
from routes.LoteAPI import loteCsw_routes
from .Planejamento.acomp_meta_plano import MetasFases_routes
from .Planejamento.TipoNotaCSW import TipoNotaCsw_routes
from .Planejamento.SaldoPlanoAnterior import SaldoPlanoAnt_routes
from .Planejamento.cronograma import cronograma_routes
from routes.routesModels.faccionista import faccionista_routes
from routes.routesModels.Meta_faccionistas import MetasFacicionista_routes
from .routesModels.LeadTimeRoutes import LeadTime_routes
from .NecessidadesDeMP.necessidadeMP import NecessidadesMP_routes
from .routesModels.FasesRoutes import Fase_routes
from .FaccionistaAPI import FaccionostaAPI_routes
from .StatusEmProcessoFaccionistaAPI import StatusFaccionostaEmProcesso_routes
from .ColecaoApi import  ColecaoAPI_routes
from .GestaoPartesAPI import GestaoPartes_routes
from .RevisorAPI import Revisores_Routes
from .LiberacaoQualidadeAPI import LiberacaoQualidade_Routes
from .MetaApi import metas_routes
from .VendasApi import vendas_routes
from .TendenciaApi import tendencia_routes
from .AnaliseMateriaisApi import analiseMP_routes
from .SimulacaoProg import simulacaoProg_routes
from .TendenciaClienteApi import tendenciaClientes_routes

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
routes_blueprint.register_blueprint(NecessidadesMP_routes)
routes_blueprint.register_blueprint(Fase_routes)
routes_blueprint.register_blueprint(FaccionostaAPI_routes)
routes_blueprint.register_blueprint(StatusFaccionostaEmProcesso_routes)
routes_blueprint.register_blueprint(ColecaoAPI_routes)
routes_blueprint.register_blueprint(GestaoPartes_routes)
routes_blueprint.register_blueprint(Revisores_Routes)
routes_blueprint.register_blueprint(LiberacaoQualidade_Routes)
routes_blueprint.register_blueprint(metas_routes)
routes_blueprint.register_blueprint(vendas_routes)
routes_blueprint.register_blueprint(tendencia_routes)
routes_blueprint.register_blueprint(analiseMP_routes)
routes_blueprint.register_blueprint(simulacaoProg_routes)
routes_blueprint.register_blueprint(tendenciaClientes_routes)
