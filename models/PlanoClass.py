import gc

import pandas as pd
import pytz
from connection import ConexaoPostgreWms, ConexaoBanco
from datetime import datetime, timedelta
from models import Lote, ProdutosClass
from models.Planejamento.loteCsw import CarregarRoteiroEngLote


class Plano():
    '''
    Classe criada para o "Plano" do PCP que é um conjunto de parametrizacoes para se fazer um planejamento.
    '''
    def __init__(self, codPlano= None ,descricaoPlano = None, iniVendas= None, fimVendas= None, iniFat= None, fimFat= None, usuarioGerador= None, codLote = None):
        '''
        Definicao do construtor: atributos do plano
        '''
        self.codPlano = codPlano
        self.descricaoPlano = descricaoPlano
        self.iniVendas = iniVendas
        self.fimVendas = fimVendas

        self.iniFat = iniFat
        if self.iniFat == None: # Atributo de Inicio do Faturamento, caso nao seja informado busca via sql na funcao obterDataInicioFatPlano()
            self.iniFat = self.obterDataInicioFatPlano()
        else:
            self.iniFat = iniFat


        self.fimFat = fimFat
        self.usuarioGerador = usuarioGerador
        self.codLote = codLote

    def inserirNovoPlano(self):
        '''
        Inserindo um novo plano

        :returns:
            status do plano no formado DATAFRAME-pandas
        '''

        # Validando se o Plano ja existe
        validador = self.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if not validador.empty:

            return pd.DataFrame([{'Status': False, 'Mensagem': 'O Plano ja existe'}])

        else:

            insert = """INSERT INTO pcp."Plano" ("codigo","descricaoPlano","inicioVenda","FimVenda","inicoFat", "finalFat", "usuarioGerador","dataGeracao") 
            values (%s, %s, %s, %s, %s, %s, %s, %s ) """

            data = self.obterdiaAtual()
            conn = ConexaoPostgreWms.conexaoInsercao()
            cur = conn.cursor()
            cur.execute(insert,
                        (self.codPlano, self.descricaoPlano, self.iniVendas, self.fimVendas, self.iniFat, self.fimFat, self.usuarioGerador, str(data),))
            conn.commit()
            cur.close()
            conn.close()

            return pd.DataFrame([{'Status': True, 'Mensagem': 'Novo Plano Criado com sucesso !'}])

    def obterdiaAtual(self):
        '''
        Método para obter a data atual do dia
        :return:
            'data de hoje no formato - %d/%m/%Y'
        '''
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%M-%D')
        return agora

    def consultarPlano(self):
        '''
        Metoto que busca todos os planos cadastrados no Sistema do PCP
        :return:
        DataFrame (em pandas) com todos os planos
        '''
        conn = ConexaoPostgreWms.conexaoEngine()
        planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)

        return planos

    def vincularLotesAoPlano(self, codPlano):
        '''
        metodo criado para vincular lotes de producao ao Plano
        :return:
        '''

    def obterDataInicioFatPlano(self):
        '''Metodo que obtem a DataInicial de faturamento do plano'''


        sql = """SELECT p."inicoFat" FROM pcp."Plano" p where codigo = %s"""
        conn = ConexaoPostgreWms.conexaoEngine()
        dataInicial =  pd.read_sql(sql,conn, params=(str(self.codPlano),))

        return dataInicial['inicoFat'][0]

    def obterDataFinalFatPlano(self):
        sql = """SELECT p."finalFat" FROM pcp."Plano" p where codigo = %s"""
        conn = ConexaoPostgreWms.conexaoEngine()
        dataInicial = pd.read_sql(sql, conn, params=(str(self.codPlano),))

        return dataInicial['finalFat'][0]

    def obterPlanos(self):
        conn = ConexaoPostgreWms.conexaoEngine()
        planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)
        planos.rename(
            columns={'codigo': '01- Codigo Plano', 'descricaoPlano': '02- Descricao do Plano',
                     'inicioVenda': '03- Inicio Venda',
                     'FimVenda': '04- Final Venda', "inicoFat": "05- Inicio Faturamento",
                     "finalFat": "06- Final Faturamento",
                     'usuarioGerador': '07- Usuario Gerador', 'dataGeracao': '08- Data Geracao'},
            inplace=True)
        planos.fillna('-', inplace=True)

        sqlLoteporPlano = """
        select
            plano as "01- Codigo Plano",
            lote,
            nomelote
        from
            "PCP".pcp."LoteporPlano"
        """

        sqlTipoNotasPlano = """select "tipo nota"||'-'||nome as "tipoNota" , plano as "01- Codigo Plano"  from pcp."tipoNotaporPlano" tnp """

        lotes = pd.read_sql(sqlLoteporPlano, conn)
        TipoNotas = pd.read_sql(sqlTipoNotasPlano, conn)

        lotes['01- Codigo Plano'] = lotes['01- Codigo Plano'].astype(str)

        merged = pd.merge(planos, lotes, on='01- Codigo Plano', how='left')
        merged = pd.merge(merged, TipoNotas, on='01- Codigo Plano', how='left')

        # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
        grouped = merged.groupby(['01- Codigo Plano', '02- Descricao do Plano', '03- Inicio Venda', '04- Final Venda',
                                  '05- Inicio Faturamento', '06- Final Faturamento', '07- Usuario Gerador',
                                  '08- Data Geracao']).agg({
            'lote': lambda x: list(x.dropna().astype(str).unique()),
            'nomelote': lambda x: list(x.dropna().astype(str).unique()),
            'tipoNota': lambda x: list(x.dropna().astype(str).unique())
        }).reset_index()

        result = []
        for index, row in grouped.iterrows():
            entry = {
                '01- Codigo Plano': row['01- Codigo Plano'],
                '02- Descricao do Plano': row['02- Descricao do Plano'],
                '03- Inicio Venda': row['03- Inicio Venda'],
                '04- Final Venda': row['04- Final Venda'],
                '05- Inicio Faturamento': row['05- Inicio Faturamento'],
                '06- Final Faturamento': row['06- Final Faturamento'],
                '07- Usuario Gerador': row['07- Usuario Gerador'],
                '08- Data Geracao': row['08- Data Geracao'],
                '09- lotes': row['lote'],
                '10- nomelote': row['nomelote'],
                '11-TipoNotas': row['tipoNota']
            }
            result.append(entry)

        return result

    def obterPlanosPlano(self):
        conn = ConexaoPostgreWms.conexaoEngine()
        planos = pd.read_sql('SELECT * FROM pcp."Plano" ORDER BY codigo ASC;', conn)
        planos.rename(
            columns={'codigo': '01- Codigo Plano', 'descricaoPlano': '02- Descricao do Plano',
                     'inicioVenda': '03- Inicio Venda',
                     'FimVenda': '04- Final Venda', "inicoFat": "05- Inicio Faturamento",
                     "finalFat": "06- Final Faturamento",
                     'usuarioGerador': '07- Usuario Gerador', 'dataGeracao': '08- Data Geracao'},
            inplace=True)
        planos.fillna('-', inplace=True)
        planos = planos[planos['01- Codigo Plano'] == self.codPlano].reset_index()
        sqlLoteporPlano = """
        select
            plano as "01- Codigo Plano",
            lote,
            nomelote
        from
            "PCP".pcp."LoteporPlano"
        """

        sqlTipoNotasPlano = """select "tipo nota"||'-'||nome as "tipoNota" , plano as "01- Codigo Plano"  from pcp."tipoNotaporPlano" tnp """

        lotes = pd.read_sql(sqlLoteporPlano, conn)
        TipoNotas = pd.read_sql(sqlTipoNotasPlano, conn)

        lotes['01- Codigo Plano'] = lotes['01- Codigo Plano'].astype(str)

        merged = pd.merge(planos, lotes, on='01- Codigo Plano', how='left')
        merged = pd.merge(merged, TipoNotas, on='01- Codigo Plano', how='left')

        # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
        grouped = merged.groupby(['01- Codigo Plano', '02- Descricao do Plano', '03- Inicio Venda', '04- Final Venda',
                                  '05- Inicio Faturamento', '06- Final Faturamento', '07- Usuario Gerador',
                                  '08- Data Geracao']).agg({
            'lote': lambda x: list(x.dropna().astype(str).unique()),
            'nomelote': lambda x: list(x.dropna().astype(str).unique()),
            'tipoNota': lambda x: list(x.dropna().astype(str).unique())
        }).reset_index()

        result = []
        for index, row in grouped.iterrows():
            entry = {
                '01- Codigo Plano': row['01- Codigo Plano'],
                '02- Descricao do Plano': row['02- Descricao do Plano'],
                '03- Inicio Venda': row['03- Inicio Venda'],
                '04- Final Venda': row['04- Final Venda'],
                '05- Inicio Faturamento': row['05- Inicio Faturamento'],
                '06- Final Faturamento': row['06- Final Faturamento'],
                '07- Usuario Gerador': row['07- Usuario Gerador'],
                '08- Data Geracao': row['08- Data Geracao'],
                '09- lotes': row['lote'],
                '10- nomelote': row['nomelote'],
                '11-TipoNotas': row['tipoNota']
            }
            result.append(entry)

        return result


    def obterNumeroSemanasVendas(self):
            '''Metodo que obtem o numero de semanas de vendas do Plano
            Calcula o número de semanas entre duas datas, considerando:
            - A semana começa na segunda-feira.
            - Se a data inicial não for uma segunda-feira, considera a primeira semana começando na data inicial.

            Parâmetros:
                ini (str): Data inicial no formato 'YYYY-MM-DD'.
                fim (str): Data final no formato 'YYYY-MM-DD'.

            Retorna:
                int: Número de semanas entre as duas datas.
            '''

            self.iniVendas, self.fimVendas = self.pesquisarInicioFimVendas()

            if self.iniVendas == '-':
                return 0
            else:

                data_ini = datetime.strptime(self.iniVendas, '%Y-%m-%d')
                data_fim = datetime.strptime(self.fimVendas, '%Y-%m-%d')

                if data_ini > data_fim:
                    raise ValueError("A data inicial deve ser anterior ou igual à data final.")

                # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
                if data_ini.weekday() != 0:  # 0 representa segunda-feira
                    proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
                else:
                    proxima_segunda = data_ini

                # Calcular o número de semanas completas a partir da próxima segunda-feira
                semanas_completas = (data_fim - proxima_segunda).days // 7

                # Verificar se existe uma semana parcial no final
                dias_restantes = (data_fim - proxima_segunda).days % 7
                semana_inicial_parcial = 1 if data_ini.weekday() != 0 else 0
                semana_final_parcial = 1 if dias_restantes > 0 else 0

                return semanas_completas + semana_inicial_parcial + semana_final_parcial

    def obterNumeroSemanasFaturamento(self):
            '''Metodo que obtem o numero de semanas de faturamento do Plano
            Calcula o número de semanas entre duas datas, considerando:
            - A semana começa na segunda-feira.
            - Se a data inicial não for uma segunda-feira, considera a primeira semana começando na data inicial.

            Parâmetros:
                ini (str): Data inicial no formato 'YYYY-MM-DD'.
                fim (str): Data final no formato 'YYYY-MM-DD'.

            Retorna:
                int: Número de semanas entre as duas datas.
            '''

            self.iniFat, self.fimFat = self.pesquisarInicioFimFat()

            if self.iniFat == '-':
                return 0
            else:

                data_ini = datetime.strptime(self.iniFat, '%Y-%m-%d')
                data_fim = datetime.strptime(self.fimFat, '%Y-%m-%d')

                if data_ini > data_fim:
                    raise ValueError("A data inicial deve ser anterior ou igual à data final.")

                # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
                if data_ini.weekday() != 0:  # 0 representa segunda-feira
                    proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
                else:
                    proxima_segunda = data_ini

                # Calcular o número de semanas completas a partir da próxima segunda-feira
                semanas_completas = (data_fim - proxima_segunda).days // 7

                # Verificar se existe uma semana parcial no final
                dias_restantes = (data_fim - proxima_segunda).days % 7
                semana_inicial_parcial = 1 if data_ini.weekday() != 0 else 0
                semana_final_parcial = 1 if dias_restantes > 0 else 0

                return semanas_completas + semana_inicial_parcial + semana_final_parcial

    def obterSemanaAtual(self):
        '''Calcula em qual semana está o dia atual dentro do intervalo de vendas.
        Caso o dia atual esteja fora do intervalo (após a data final), retorna "finalizado".

        Retorna:
            int ou str: Número da semana atual ou "finalizado".
        '''
        self.iniVendas, self.fimVendas = self.pesquisarInicioFimVendas()

        if self.iniVendas == '-':
            return "finalizado"

        data_ini = datetime.strptime(self.iniVendas, '%Y-%m-%d')
        data_fim = datetime.strptime(self.fimVendas, '%Y-%m-%d')
        hoje = datetime.today()

        if data_ini > data_fim:
            raise ValueError("A data inicial deve ser anterior ou igual à data final.")

        if hoje > data_fim:
            return "finalizado"

        # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
        if data_ini.weekday() != 0:  # 0 representa segunda-feira
            proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
        else:
            proxima_segunda = data_ini

        # Calcular a diferença de semanas entre a data inicial ajustada e hoje
        semanas_completas = (hoje - proxima_segunda).days // 7

        # Verificar se hoje está na primeira semana parcial
        semana_inicial_parcial = 1 if hoje < proxima_segunda and hoje >= data_ini else 0

        # Retornar o número da semana atual
        return semanas_completas + semana_inicial_parcial + 1

    def obterSemanaAtualFat(self):
        '''Calcula em qual semana está o dia atual dentro do intervalo de vendas.
        Caso o dia atual esteja fora do intervalo (após a data final), retorna "finalizado".

        Retorna:
            int ou str: Número da semana atual ou "finalizado".
        '''
        self.iniFat, self.fimFat = self.pesquisarInicioFimFat()

        if self.iniFat == '-':
            return "finalizado"

        data_ini = datetime.strptime(self.iniFat, '%Y-%m-%d')
        data_fim = datetime.strptime(self.fimFat, '%Y-%m-%d')
        hoje = datetime.today()

        if data_ini > data_fim:
            raise ValueError("A data inicial deve ser anterior ou igual à data final.")

        if hoje > data_fim:
            return "finalizado"

        # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
        if data_ini.weekday() != 0:  # 0 representa segunda-feira
            proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
        else:
            proxima_segunda = data_ini

        # Calcular a diferença de semanas entre a data inicial ajustada e hoje
        semanas_completas = (hoje - proxima_segunda).days // 7

        # Verificar se hoje está na primeira semana parcial
        semana_inicial_parcial = 1 if hoje < proxima_segunda and hoje >= data_ini else 0

        # Retornar o número da semana atual
        return semanas_completas + semana_inicial_parcial + 1

    def pesquisarInicioFimVendas(self):
        '''metodo que pesquisa o inicio e o fim das vendas passeado no codPlano'''

        sql = """
        select 
            "inicioVenda","FimVenda"
        from
            "PCP".pcp."Plano"
        where
            "codigo" = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        if not consulta.empty:

            inicioVenda = consulta['inicioVenda'][0]
            FimVenda = consulta['FimVenda'][0]

            return inicioVenda, FimVenda

        else:
            return '-', '-'

    def pesquisarInicioFimFat(self):
        '''metodo que pesquisa o inicio e o fim das vendas passeado no codPlano'''

        sql = """
        select 
            "inicoFat","finalFat"
        from
            "PCP".pcp."Plano"
        where
            "codigo" = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        if not consulta.empty:

            inicoFat = consulta['inicoFat'][0]
            finalFat = consulta['finalFat'][0]

            return inicoFat, finalFat

        else:
            return '-', '-'

    def AlterPlano(self):
        # Validando se o Plano ja existe
        validador = self.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if validador.empty:

            return pd.DataFrame([{'Status': False, 'Mensagem': 'O Plano Informado nao existe'}])
        else:
            descricaoPlanoAtual = validador['descricaoPlano'][0]
            if descricaoPlanoAtual == self.descricaoPlano or self.descricaoPlano == '-':
                self.descricaoPlano = descricaoPlanoAtual


            iniVendasAtual = validador['inicioVenda'][0]
            if iniVendasAtual == self.iniVendas or self.iniVendas == '-':
                self.iniVendas = iniVendasAtual


            FimVendaAtual = validador['FimVenda'][0]
            if FimVendaAtual == self.fimVendas or self.fimVendas == '-':
                self.fimVendas = FimVendaAtual


            inicoFatAtual = validador['inicoFat'][0]
            if inicoFatAtual == self.iniFat or self.iniFat == '-':
                self.iniFat = inicoFatAtual

            finalFatAtual = validador['finalFat'][0]
            if finalFatAtual == self.fimFat or self.fimFat == '-':
                self.fimFat = finalFatAtual

            update = """update "PCP".pcp."Plano"  set "descricaoPlano" = %s , "inicioVenda" = %s , "FimVenda" = %s , "inicoFat" = %s , "finalFat" = %s
            where "codigo" = %s
            """

            conn = ConexaoPostgreWms.conexaoInsercao()
            cur = conn.cursor()
            cur.execute(update, (self.descricaoPlano, self.iniVendas, self.fimVendas, self.iniFat, self.fimFat, self.codPlano,))
            conn.commit()
            cur.close()
            conn.close()
            return pd.DataFrame([{'Status': True, 'Mensagem': 'O Plano foi alterado com sucesso !'}])

    def vinculandoLotesAoPlano(self, arrayCodLoteCsw):
        '''Metodo que vincula lotes ao plano'''
        empresa = '1'
        # Validando se o Plano ja existe
        validador = self.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if validador.empty:

            return pd.DataFrame([{'Status': False, 'Mensagem': f'O Plano {self.codPlano} NAO existe'}])
        else:

            # Deletando caso ja exista vinculo do lote no planto
            deleteVinculo = """Delete from pcp."LoteporPlano" where "lote" = %s AND plano = %s """
            insert = """insert into pcp."LoteporPlano" ("empresa", "plano","lote", "nomelote") values (%s, %s, %s, %s  )"""
            delete = """Delete from pcp.lote_itens where "codLote" = %s """
            conn = ConexaoPostgreWms.conexaoInsercao()
            cur = conn.cursor()

            for lote in arrayCodLoteCsw:
                self.codLote = lote
                nomelote = Lote.Lote(self.codLote).consultarLoteEspecificoCsw()
                cur.execute(deleteVinculo, (lote, self.codPlano,))
                conn.commit()
                cur.execute(insert, (empresa, self.codPlano, lote, nomelote,))
                conn.commit()
                cur.execute(delete, (lote,))
                conn.commit()

            cur.close()
            conn.close()

            '''Metodo que insere no banco de dados os itens do lote '''
            self.ExplodindoAsReferenciasLote(arrayCodLoteCsw)

            return pd.DataFrame([{'Status': True, 'Mensagem': 'Lotes adicionados ao Plano com sucesso !'}])

    def desvincularLotesAoPlano(self,arrayCodLoteCsw):

        empresa = '1'
        # Validando se o Plano ja existe
        validador = self.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if validador.empty:

            return pd.DataFrame([{'Status': False, 'Mensagem': f'O Plano {self.codPlano} NAO existe'}])
        else:
            for lote in arrayCodLoteCsw:
                self.codLote = lote
                Lote.Lote(self.codLote,'1',self.codPlano).desvincularLotePlano()

            return pd.DataFrame([{'Status': True, 'Mensagem': 'Lotes Desvinculados do Plano com sucesso !'}])


    def pesquisarTipoNotasPlano(self):
        '''Metodo utilizado para obter os tipo de notas de um determinado plano'''

        sql = """
            select
	            "tipo nota" as "codTipoNota"
            from
	            "PCP".pcp."tipoNotaporPlano" tnp
            where
	            plano = %s
        """

        conn = ConexaoPostgreWms.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        return consulta

    def ExplodindoAsReferenciasLote(self, arrayCodLoteCsw):
        nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
        novo = ", ".join(nomes_com_aspas)
        print(novo)
        sqlLotes = """
        select Empresa , t.codLote, codengenharia, t.codSeqTamanho , t.codSortimento , t.qtdePecasImplementadas as previsao FROM tcl.LoteSeqTamanho t
        WHERE t.Empresa = """ + '1' + """and t.codLote in (""" + novo + """) 
        """

        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlLotes)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        # Implantando no banco de dados do Pcp
        ConexaoPostgreWms.Funcao_InserirOFF(lotes, lotes['codLote'].size, 'lote_itens', 'append')
        ProdutosClass.Produto().RecarregarItens()
        CarregarRoteiroEngLote('1', arrayCodLoteCsw)

        return lotes