import pandas as pd
import pytz
from datetime import datetime
from connection import ConexaoPostgreWms


class Plano():
    '''
    Classe criada para o "Plano" do PCP que é um conjunto de parametrizacoes para se fazer um planejamento.
    '''
    def __init__(self, codPlano= None ,descricaoPlano = None, iniVendas= None, fimVendas= None, iniFat= None, fimFat= None, usuarioGerador= None):
        '''
        Definicao do construtor: atributos do plano
        '''
        self.codPlano = codPlano
        self.descricaoPlano = descricaoPlano
        self.iniVendas = iniVendas
        self.fimVendas = fimVendas
        self.iniFat = iniFat
        self.fimFat = fimFat
        self.usuarioGerador = usuarioGerador

    def InserirNovoPlano(self):
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
            print('data' + data)
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
        agora = agora.strftime('%d/%m/%Y')
        return agora

    def consultarPlano(self):
        '''
        Medoto que busca todos os planos cadastrados no Sistema do PCP
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

    def obterDataInicioPlano(self):
        sql = """SELECT p."inicoFat" FROM pcp."Plano" where codigo = %s"""
        conn = ConexaoPostgreWms.conexaoEngine()
        dataInicial =  pd.read_sql(sql,conn, params=(self.codPlano))

        return dataInicial['inicoFat'][0]

    def obterDataFinalPlano(self):
        sql = """SELECT p."finalFat" FROM pcp."Plano" where codigo = %s"""
        conn = ConexaoPostgreWms.conexaoEngine()
        dataInicial = pd.read_sql(sql, conn, params=(self.codPlano))

        return dataInicial['finalFat'][0]