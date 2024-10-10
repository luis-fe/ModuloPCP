import gc
from datetime import datetime
import pytz
from models import FaccionistaCategoria as FC
from models import Faccionista as fac
from connection import ConexaoPostgreWms,ConexaoBanco
import pandas as pd

class StatusOpsEmProcesso():
    '''Classe utilizada para Gerenciamento do Status das OPs em processo em faccionistas'''

    def __init__(self, nomeFaccionista = None, statusTerceirizado= None, numeroOP= None, usuario= None,
                 justificativa= None, dataMarcacao= None, statusAtualizacao = None, nomecategoria = None):
        self.numeroOP = numeroOP
        self.nomecategoria = nomecategoria
        self.nomeFaccionista = nomeFaccionista


    def obterDataHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora

    def obterRemessasDistribuicaoCSW(self):
        '''Metodo que retorna a carga por faccionista e categoria, no banco de dados do CSW'''

        sql = """
         SELECT 
             op.numeroOP, 
             op.codProduto, 
             d.codOP, 
             d.codFac as codfaccionista,
             l.qtdPecasRem as carga,
             op.codPrioridadeOP, 
             op.codTipoOP,  
             d.datLib as dataEnvio,
             e.descricao as nome,
             (SELECT p.descricao from tcp.PrioridadeOP p WHERE p.empresa = 1 and p.codPrioridadeOP =op.codPrioridadeOP) as prioridade  
         FROM 
             tco.OrdemProd op 
         left join 
             tct.RemessaOPsDistribuicao d 
             on d.Empresa = 1 and 
             d.codOP = op.numeroOP and 
             d.situac = 2 and d.codFase = op.codFaseAtual 
         left join 
             tct.RemessasLoteOP l 
             on l.Empresa = d.Empresa  
             and l.codRemessa = d.numRem 
         join 
             tcp.Engenharia e on 
             e.codEmpresa = 1 and 
             e.codEngenharia = op.codProduto 
         WHERE 
             op.codEmpresa =1 and 
             op.situacao =3 and 
             op.codFaseAtual in (455, 459, 429)
             """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                cargaFac = pd.DataFrame(rows, columns=colunas)
        # Libera memória manualmente
        del rows
        gc.collect()

        cargaFac['categoria'] = '-'
        cargaFac['categoria'] = cargaFac['nome'].apply(self.mapear_categoria)

        # Aplicar a contagem somente nas linhas onde codPrioridadeOP == 6

        cargaFac['Mostruario'] = cargaFac.groupby('codfaccionista')['codTipoOP'].apply(
            lambda x: (x == 6).sum()).reindex(cargaFac['codfaccionista']).values
        cargaFac['Urgente'] = cargaFac.groupby('codfaccionista')['prioridade'].apply(
            lambda x: (x == 'URGENTE').sum()).reindex(cargaFac['codfaccionista']).values
        cargaFac['FAT Atrasado'] = cargaFac.groupby('codfaccionista')['prioridade'].apply(
            lambda x: (x == 'FAT ATRASADO').sum()).reindex(cargaFac['codfaccionista']).values
        cargaFac['P_Faturamento'] = cargaFac.groupby('codfaccionista')['prioridade'].apply(
            lambda x: (x == 'P\ FATURAMENTO').sum()).reindex(cargaFac['codfaccionista']).values

        cargaFac['OP'] = cargaFac.groupby('codfaccionista')['codOP'].transform('count')

        return cargaFac



    def obterRemessasDistribuicaoCSWOPEspecifica(self):
        '''Metodo que retorna a carga por faccionista e categoria, no banco de dados do CSW'''

        sql = """
         SELECT 
             op.numeroOP, 
             op.codProduto, 
             d.codFac as codfaccionista,
             l.qtdPecasRem as carga,
             op.codPrioridadeOP, 
             op.codTipoOP,  
             d.datLib as dataEnvio,
             e.descricao as nome,
             (SELECT p.descricao from tcp.PrioridadeOP p WHERE p.empresa = 1 and p.codPrioridadeOP =op.codPrioridadeOP) as prioridade  
         FROM 
             tco.OrdemProd op 
         left join 
             tct.RemessaOPsDistribuicao d 
             on d.Empresa = 1 and 
             d.codOP = op.numeroOP and 
             d.situac = 2 and d.codFase = op.codFaseAtual 
         left join 
             tct.RemessasLoteOP l 
             on l.Empresa = d.Empresa  
             and l.codRemessa = d.numRem 
         join 
             tcp.Engenharia e on 
             e.codEmpresa = 1 and 
             e.codEngenharia = op.codProduto 
         WHERE 
             op.codEmpresa =1 and 
             op.situacao =3 and 
             op.codFaseAtual in (455, 459, 429)
             and op.numeroOP like '%"""+self.numeroOP+"""%'"""

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                cargaFac = pd.DataFrame(rows, columns=colunas)
        # Libera memória manualmente
        del rows
        gc.collect()

        cargaFac['categoria'] = '-'
        cargaFac['categoria'] = cargaFac['nome'].apply(self.mapear_categoria)



        return cargaFac

    def obterFaccionistasCategoria(self):
        consultaCategoriaFacc = FC.FaccionistaCategoria(None, self.nomecategoria).obterFaccionistasCategoria()
        return consultaCategoriaFacc
    def obterFaccionistaGeral(self):
        consulta =  FC.FaccionistaCategoria(None, self.nomecategoria).obterFaccionistasCategoriaPorFac()
        return consulta


    def getOPsEmProcessoCategoria(self):
        consultarOPCsw = self.obterRemessasDistribuicaoCSW()

        # Nessa etapa verificamos se a Categoria esta vazia ou foi informada para poder informar o que o usuario deseja
        if self.nomecategoria != None:
            consultaCategoriaFacc = self.obterFaccionistasCategoria()
            consultarOPCsw = consultarOPCsw[consultarOPCsw['categoria'] == self.nomecategoria]
        else:
            consultaCategoriaFacc = self.obterFaccionistaGeral()

        consultarOPCsw['codfaccionista'] = consultarOPCsw['codfaccionista'].astype(str).str.replace(r'\.0$', '', regex=True)

        consultarOPCsw1 = consultarOPCsw.groupby(['categoria', 'codfaccionista']).agg(
            {'carga': 'sum', 'OP': 'first', 'Mostruario': 'first', 'Urgente': 'first',
             'FAT Atrasado': 'first', 'P_Faturamento': 'first'}).reset_index()

        consulta = pd.merge(consultaCategoriaFacc, consultarOPCsw1, on=['codfaccionista', 'categoria'], how='right')
        consulta['carga'].fillna(0, inplace=True)
        consulta = consulta[consulta['carga'] > 0]
        consulta.fillna("-", inplace=True)



        if self.nomeFaccionista == None:

            carga = pd.merge(consultaCategoriaFacc, consultarOPCsw, on=['codfaccionista', 'categoria'], how='right')
            carga['leadtime'].fillna(0,inplace=True)
            carga['dataPrevista'] = pd.to_datetime(carga['dataEnvio'])
            # Tratando apenas os valores válidos de leadtime (não-negativos)
            carga['dataPrevista'] = carga.apply(
                lambda row: row['dataPrevista'] + pd.to_timedelta(row['leadtime'], unit='D'), axis=1)
            carga['dataPrevista'] = carga['dataPrevista'].dt.strftime('%Y-%m-%d')

            carga.drop(['FAT Atrasado', 'Mostruario', 'OP', 'P_Faturamento', 'Urgente'], axis=1, inplace=True)

            getStatus = self.getstatusOp()
            carga = pd.merge(carga,getStatus,on='numeroOP',how='left')

            carga.fillna('-',inplace=True)

            data = {
                '1- Resumo:': consulta.to_dict(orient='records'),
                '2- Detalhamento:': carga.to_dict(orient='records')
            }

            return pd.DataFrame([data])

        else:
            codigosFaccionista = fac.Faccionista(None,self.nomeFaccionista).obterCodigosFaccionista()

            carga = pd.merge(consultarOPCsw,codigosFaccionista,on='codfaccionista')
            print(carga)
            carga = pd.merge(consultaCategoriaFacc, carga, on=['codfaccionista', 'categoria'], how='right')
            carga['leadtime'].fillna(0,inplace=True)
            consulta = consulta[consulta['apelidofaccionista'] == self.nomeFaccionista]

            carga['dataPrevista'] = pd.to_datetime(carga['dataEnvio'])
            # Tratando apenas os valores válidos de leadtime (não-negativos)
            carga['dataPrevista'] = carga.apply(
                lambda row: row['dataPrevista'] + pd.to_timedelta(row['leadtime'], unit='D'), axis=1)
            carga['dataPrevista'] = carga['dataPrevista'].dt.strftime('%Y-%m-%d')

            carga.drop(['FAT Atrasado', 'Mostruario', 'OP', 'P_Faturamento', 'Urgente'], axis=1, inplace=True)
            getStatus = self.getstatusOp()
            carga = pd.merge(carga, getStatus, on='numeroOP', how='left')
            carga.fillna('-', inplace=True)

            data = {
                '1- Resumo:': consulta.to_dict(orient='records'),
                '2- Detalhamento:': carga.to_dict(orient='records')
            }

            return pd.DataFrame([data])
    def mapear_categoria(self,nome):
        categorias_map = {
            'CAMISA': 'CAMISA',
            'POLO': 'POLO',
            'BATA': 'CAMISA',
            'TRICOT': 'TRICOT',
            'BONE': 'BONE',
            'CARTEIRA': 'CARTEIRA',
            'TSHIRT': 'CAMISETA',
            'REGATA': 'CAMISETA',
            'BLUSAO': 'AGASALHOS',
            'BABY': 'CAMISETA',
            'JAQUETA': 'JAQUETA',
            'CINTO': 'CINTO',
            'PORTA CAR': 'CARTEIRA',
            'CUECA': 'CUECA',
            'MEIA': 'MEIA',
            'SUNGA': 'SUNGA',
            'SHORT': 'SHORT',
            'BERMUDA': 'BERMUDA'
        }
        for chave, valor in categorias_map.items():
            if chave in nome.upper():
                return valor
        return '-'

    def getstatusOp(self):

        select = """
                select
                    numeroop as "numeroOP",
                    justificativa,
                    status,
                    "dataPrevAtualizada"
				from
					"PCP".pcp."StatusTerceirizadoOP"
				where 
					"statusAtualizacao" = 'atual'
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(select,conn)
        return consulta

    def filtrandoOPEspecifica(self):
        consultarOPCsw = self.obterRemessasDistribuicaoCSWOPEspecifica()
        consultaCategoriaFacc = self.obterFaccionistaGeral()

        filtro = pd.merge(consultarOPCsw,consultaCategoriaFacc,on='codfaccionista',how='left')
        getStatus = self.getstatusOp()
        filtro = pd.merge(filtro, getStatus, on='numeroOP', how='left')
        return filtro





