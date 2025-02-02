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
                 justificativa= None, dataMarcacao= None, statusAtualizacao = None, nomecategoria = None, congelarDashboard = False, dataPrevAtualizada = None ):
        self.numeroOP = numeroOP
        self.nomecategoria = nomecategoria
        self.nomeFaccionista = nomeFaccionista
        self.statusTerceirizado = statusTerceirizado
        self.usuario = usuario
        self.justificativa = justificativa
        self.dataMarcacao = self.obterDataHoraAtual()
        self.congelarDashboard = congelarDashboard
        self.dataPrevAtualizada = dataPrevAtualizada


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
             op.codFaseAtual in (455, 459, 429, 453)
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


        return cargaFac

    def obterFaccionistasCategoria(self):
        consultaCategoriaFacc = FC.FaccionistaCategoria(None, self.nomecategoria).obterFaccionistasCategoria()
        return consultaCategoriaFacc
    def obterFaccionistaGeral(self):
        consulta =  FC.FaccionistaCategoria(None, self.nomecategoria).obterFaccionistasCategoriaPorFac()
        return consulta


    def getOPsEmProcessoCategoria(self):
        '''Metodo que retorna detalhadamente todas as Ops em fase de terceiros '''

        # 1 obtendo do csw as Ops aberto por faccinoista
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

        consulta = pd.merge( consultarOPCsw1,consultaCategoriaFacc, on=['codfaccionista', 'categoria'], how='left')
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
        consultarOPCsw['codfaccionista'] = consultarOPCsw['codfaccionista'].astype(str).str.replace(r'\.0$', '', regex=True)


        filtro = pd.merge(consultarOPCsw,consultaCategoriaFacc,on='codfaccionista',how='left')
        getStatus = self.getstatusOp()
        filtro = pd.merge(filtro, getStatus, on='numeroOP', how='left')
        filtro.fillna('-',inplace=True)
        return filtro

    def post_apontarStatusOP(self):
        '''Metodo utilizado para apontar o status'''

        #Verifica se esta vazio o apontamento da OP em especifico

        consulta = """
                        select
                    numeroop as "numeroOP",
                    justificativa,
                    status,
                    "dataPrevAtualizada"
				from
					"PCP".pcp."StatusTerceirizadoOP"
				where 
					 numeroop = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta,conn,params=(self.numeroOP,))

        if consulta.empty:
            insert = """INSERT INTO "PCP".pcp."StatusTerceirizadoOP" (usuario,"dataMarcacao" , numeroop, justificativa, status, "statusAtualizacao" )
            values ( %s, %s, %s, %s, %s, %s)
            """






            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(insert, (self.usuario, self.dataMarcacao,self.numeroOP, self.justificativa, self.statusTerceirizado,'atual'))
                    connInsert.commit()



            return pd.DataFrame([{'Status':True,'Mensagem':'Apontado com sucesso'}])



        else:
            consulta = """
                            select
                                numeroop as "numeroOP",
                                justificativa,
                                status,
                                "dataPrevAtualizada"
            				from
            					"PCP".pcp."StatusTerceirizadoOP"
            				where 
            					"statusAtualizacao" = 'atual'
            					and numeroop = %s and status = %s
                    """
            conn = ConexaoPostgreWms.conexaoEngine()
            consulta = pd.read_sql(consulta, conn, params=(self.numeroOP,self.statusTerceirizado))



            if consulta.empty:
                updateHistorico = """
                 update 
                    "PCP".pcp."StatusTerceirizadoOP"
                 set 
                    "statusAtualizacao" = 'Historico'
                 where 
                    numeroop = %s
                """

                insert = """INSERT INTO "PCP".pcp."StatusTerceirizadoOP" (usuario,"dataMarcacao" , numeroop, justificativa, status, "statusAtualizacao" )
                values ( %s, %s, %s, %s, %s, %s)
                """

                with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                    with connInsert.cursor() as curr:

                        curr.execute(updateHistorico,(self.numeroOP,))
                        connInsert.commit()


                        curr.execute(insert, (
                        self.usuario, self.dataMarcacao, self.numeroOP, self.justificativa, self.statusTerceirizado,
                        'atual'))
                        connInsert.commit()

                return pd.DataFrame([{'Status': True, 'Mensagem': 'Apontado com sucesso'}])



            else:

                updateHistorico = """
                 update 
                    "PCP".pcp."StatusTerceirizadoOP"
                 set 
                    "statusAtualizacao" = 'Historico'
                 where 
                    numeroop = %s
                """

                insert = """INSERT INTO "PCP".pcp."StatusTerceirizadoOP" (usuario,"dataMarcacao" , numeroop, justificativa, status, "statusAtualizacao" )
                values ( %s, %s, %s, %s, %s, %s)
                """

                with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                    with connInsert.cursor() as curr:
                        curr.execute(updateHistorico, (self.numeroOP,))
                        connInsert.commit()

                        curr.execute(insert, (
                            self.usuario, self.dataMarcacao, self.numeroOP, self.justificativa, self.statusTerceirizado,
                            'atual'))
                        connInsert.commit()

                return pd.DataFrame([{'Status': True, 'Mensagem': 'Apontado com sucesso'}])

    def post_apontarDataPrev(self):
        '''Metodo utilizado para apontar o status a novel Gerencial'''

        # Verifica se esta vazio o apontamento da OP em especifico

        consulta = """
                        select
                    numeroop as "numeroOP",
                    justificativa,
                    status,
                    "dataPrevAtualizada"
                from
                    "PCP".pcp."StatusTerceirizadoOP"
                where 
                     numeroop = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta, conn, params=(self.numeroOP,))

        if consulta.empty:
            insert = """INSERT INTO "PCP".pcp."StatusTerceirizadoOP" (usuario,"dataMarcacao" , numeroop, justificativa, status, "statusAtualizacao","dataPrevAtualizada" )
            values ( %s, %s, %s, %s, %s, %s, %s)
            """

            with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                with connInsert.cursor() as curr:
                    curr.execute(insert, (
                    self.usuario, self.dataMarcacao, self.numeroOP, self.justificativa, self.statusTerceirizado,
                    'atual',self.dataPrevAtualizada))
                    connInsert.commit()

            return pd.DataFrame([{'Status': True, 'Mensagem': 'Nova Data Prev  atualizada com sucesso'}])

        else:
                update = """
                 update 
                    "PCP".pcp."StatusTerceirizadoOP"
                 set 
                    "dataPrevAtualizada" = %s
                 where 
                    numeroop = %s
                """

                with ConexaoPostgreWms.conexaoInsercao() as connInsert:
                    with connInsert.cursor() as curr:
                        curr.execute(update, (self.dataPrevAtualizada, self.numeroOP,))
                        connInsert.commit()

                return pd.DataFrame([{'Status': True, 'Mensagem': 'Nova Data Prev  atualizada com sucesso'}])

    def dashboardPecasFaccionista(self):
        '''Metodo da Classe que retorna um dashboard com a informacao da carga por faccionista em aberto '''

        # Caso o usuario passe o congelamento como falso, é feito uma consulta dianmica com o banco do ERP , demorando mais tempo:
        if self.congelarDashboard == False:
            #1 Obtem a carga dos faccionista no CSW
            obterResumo = self.obterRemessasDistribuicaoCSW()
            #2 Obtem no banco da Plataforma o status das OPs
            obterStatus = self.getstatusOp()
            #3 realiza um merge para concatenar as informacoes a nivel de OP
            obterResumo = pd.merge(obterResumo, obterStatus , on='numeroOP',how = 'left')
            #4 realiza o tratamento do status nao informado e trata o codfaccionsta no dataframe obterResumo
            obterResumo['status'].fillna('NaoInformado', inplace =True)
            obterResumo['codfaccionista'] = obterResumo['codfaccionista'].astype(str).str.replace(r'\.0$', '', regex=True)

            #5 resume o status a nivel de categoira e codfaccionsita
            resumoStatus = obterResumo.groupby(['status','categoria','codfaccionista']).agg(
                {'carga': 'sum'}).reset_index()


            #6 verifica se tem filtro no nivel de categoria
            if self.nomecategoria != None and self.nomecategoria != '':
                obterResumo = obterResumo[obterResumo['categoria']==self.nomecategoria]


            #7 verifica se tem filtro nivel apelido faccionista
            if self.nomeFaccionista != None and self.nomeFaccionista != '':
                codigosFaccionista = fac.Faccionista(None, self.nomeFaccionista).obterCodigosFaccionista()

                obterResumo = pd.merge(obterResumo,codigosFaccionista,on='codfaccionista')
                resumoStatus = pd.merge(resumoStatus,codigosFaccionista,on='codfaccionista')

                totalOps = obterResumo['numeroOP'].count()
            else:
                totalOps = obterResumo['numeroOP'].count()




            obterResumo =  obterResumo.groupby(['codfaccionista','categoria']).agg(
                {'carga': 'sum'}).reset_index()

            consultaCategoriaFacc = self.obterFaccionistaGeral()

            consulta = pd.merge(obterResumo, consultaCategoriaFacc, on=['codfaccionista','categoria'], how='left')

            if self.nomecategoria != None and self.nomecategoria != '':
                consulta = consulta[consulta['categoria']==self.nomecategoria]

                resumoStatus = resumoStatus.groupby(['status']).agg(
                    {'carga': 'sum'}).reset_index()

            if self.nomeFaccionista != None and self.nomeFaccionista != '':
                consulta = consulta.groupby(['codfaccionista', 'apelidofaccionista','categoria']).agg(
                    {'carga': 'sum'}).reset_index()

                resumoStatus = resumoStatus.groupby(['status']).agg(
                    {'carga': 'sum'}).reset_index()

            if (self.nomeFaccionista == None or self.nomeFaccionista == '') and (self.nomecategoria == None or self.nomecategoria == ''):
                self.backupDadosDashbord(consulta,resumoStatus)
                resumoStatus = resumoStatus.groupby(['status']).agg(
                    {'carga': 'sum'}).reset_index()
                consulta.drop(['dataHora'], axis=1, inplace=True)

            consulta['carga'].fillna(0, inplace=True)
            consulta = consulta[consulta['carga'] > 0].reset_index()
            consulta.fillna("-", inplace=True)

            resumoCategoria = consulta.groupby(['categoria']).agg(
                {'carga': 'sum'}).reset_index()





            consulta.drop(['categoria'], axis=1, inplace=True)

            totalPecas = consulta['carga'].sum()


            data = {
                    '1- TotalPeças:': f'{totalPecas} pçs',
                    '2- TotalOPs':f'{totalOps}',
                    '3- Distribuicao:': consulta.to_dict(orient='records'),
                    '3.1- ResumoCategoria': resumoCategoria.to_dict(orient='records'),
                    '3.2- ResumoStatus': resumoStatus.to_dict(orient='records')
            }




            return pd.DataFrame([data])

        # caso o congelamento esteja marcado como falso, retorna o congelamento do calculo
        else:

            consulta =  self.carregarBackup()
            resumoStatus1 = self.carregarBackupStatus()

            if self.nomecategoria != None and self.nomecategoria != '':
                resumoStatus1 = resumoStatus1[resumoStatus1['categoria'] == self.nomecategoria]

                consulta = resumoStatus1.groupby(['categoria', 'codfaccionista', 'apelidofaccionista']).apply(
                    lambda x: pd.Series({
                        'carga': x['carga'].sum(),
                        'status_resumo': {
                            'NaoInformado': x.loc[x['status'] == 'NaoInformado', 'carga'].sum(),
                            'Nao Iniciada': x.loc[x['status'] == 'Nao Iniciada', 'carga'].sum(),
                            'Preparacao': x.loc[x['status'] == 'Preparacao', 'carga'].sum(),
                            'Costurando': x.loc[x['status'] == 'Costurando', 'carga'].sum(),
                            'Acabamento': x.loc[x['status'] == 'Acabamento', 'carga'].sum(),
                            'Aguardando Reposicao': x.loc[x['status'] == 'Aguardando Reposicao', 'carga'].sum(),
                            'Finalizado': x.loc[x['status'] == 'Finalizado', 'carga'].sum(),
                            'Recolhido': x.loc[x['status'] == 'Recolhido', 'carga'].sum(),
                            'Consertando': x.loc[x['status'] == 'Consertando', 'carga'].sum()

                        }
                    })
                ).reset_index()

                resumoStatus = resumoStatus1.groupby(['status']).agg(
                    {'carga': 'sum'}).reset_index()


            if self.nomeFaccionista != None and self.nomeFaccionista != '':
                codigosFaccionista = fac.Faccionista(None, self.nomeFaccionista).obterCodigosFaccionista()
                consulta = pd.merge(consulta, codigosFaccionista, on='codfaccionista')
                resumoStatus = pd.merge(resumoStatus1, codigosFaccionista, on='codfaccionista')
                resumoStatus = resumoStatus.groupby(['status']).agg(
                    {'carga': 'sum'}).reset_index()


            if (self.nomeFaccionista == None or self.nomeFaccionista == '') and (self.nomecategoria == None or self.nomecategoria == ''):

                resumoStatus = resumoStatus1.groupby(['status']).agg(
                    {'carga': 'sum'}).reset_index()


            consulta['carga'].fillna(0, inplace=True)
            consulta = consulta[consulta['carga'] > 0].reset_index()
            consulta.fillna("-", inplace=True)

            resumoCategoria = consulta.groupby(['categoria']).agg(
                {'carga': 'sum'}).reset_index()



            consulta.drop(['categoria'], axis=1, inplace=True)

            totalPecas = consulta['carga'].sum()

            data = {
                '1- TotalPeças:': f'{totalPecas} pçs',
                '2- TotalOPs': f'-',
                '3- Distribuicao:': consulta.to_dict(orient='records'),
                '3.1- ResumoCategoria': resumoCategoria.to_dict(orient='records'),
                '3.2- ResumoStatus': resumoStatus.to_dict(orient='records')
            }

            return pd.DataFrame([data])

    def backupDadosDashbord(self, dataFrame, dataframe2):
        '''Metodo utilizado para deixar a api de renderizacao mais rapido dos dashboards '''
        dataFrame['dataHora'] = self.obterDataHoraAtual()
        dataframe2['dataHora'] = self.obterDataHoraAtual()

        ConexaoPostgreWms.Funcao_InserirBackup(dataFrame,dataFrame['carga'].size,"backupDashFac","replace")
        ConexaoPostgreWms.Funcao_InserirBackup(dataframe2,dataframe2['categoria'].size,"backupDashFacStatus","replace")



    def carregarBackup(self):

        consulta = """
        select
	        apelidofaccionista ,
	        categoria ,
	        codfaccionista ,
	        carga
        from
	        backup."backupDashFac" bdf
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta,conn)
        return consulta


    def carregarBackupStatus(self):

        consulta = """
        select
            status,
            categoria,
            carga,
            f.codfaccionista,
            f.apelidofaccionista
        from
            backup."backupDashFacStatus" bdf
        inner join 
            pcp.faccionista f 
            on f.codfaccionista = bdf.codfaccionista 
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(consulta,conn)
        return consulta



    def atualizaJustificativaNoGestaoOP(self):

        justificativa = str(self.statusTerceirizado)+':'+str(self.justificativa)

        # atualizacao tambem no painel de Gestao das OPs
        SelectmoduloJustificativaPainelOP = """
                   select 
                       * 
                   from 
                       "PCP".pcp.justificativa j 
                       where fase in ('429') and ordemprod = %s
                   """
        conn = ConexaoPostgreWms.conexaoEngine()
        SelectmoduloJustificativaPainelOP = pd.read_sql(SelectmoduloJustificativaPainelOP, conn,
                                                        params=(self.numeroOP,))
        if SelectmoduloJustificativaPainelOP.empty:

            insert2 = """
                       insert into "PCP".pcp.justificativa (justificativa, fase, ordemprod) values ( %s, '429', %s )
                       """
        else:
            insert2 = """
                       update "PCP".pcp.justificativa 
                       set justificativa = %s
                       where ordemprod = %s and fase = '429'
                       """

        with ConexaoPostgreWms.conexaoInsercao() as connInsert:
            with connInsert.cursor() as curr:
                curr.execute(insert2, (justificativa, self.numeroOP,))
                connInsert.commit()












