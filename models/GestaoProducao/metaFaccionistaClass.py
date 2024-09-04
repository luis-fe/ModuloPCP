import gc
from models.GestaoOPAberto import realizadoFases
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
import numpy as np
import os

class MetasFaccionistas:
    def __init__(self, codigoPlano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado):
        self.codigoPlano = codigoPlano #Codigo do Plano Escolhido
        self.arrayCodLoteCsw = arrayCodLoteCsw # codigo do Lote escolhido
        self.dataMovFaseIni = dataMovFaseIni # Data Inicio do Realizado
        self.dataMovFaseFim = dataMovFaseFim # Data Final do Realizado
        self.congelado = congelado # O usuario da api quer passar congelado ou nao
        self.conn = ConexaoPostgreWms.conexaoEngine() # Conexao com o banco de dados

    def carregarBackupPlanoFac(self):
        sql = """select * from "backup"."metaCategoria" where "plano" = %s and "codLote" = %s """
        codLote = self.arrayCodLoteCsw[0]
        backupPLano = pd.read_sql(sql, self.conn, params=(self.codigoPlano, codLote))
        return backupPLano

    def carregar_capacidades(self, DataFrameBackupPlano):
        sql = """select nomecategoria as categoria, fc.codfaccionista, "Capacidade/dia"::int, nomefaccionista as nomefaccionistaCsw from "PCP".pcp."faccaoCategoria" fc
        inner join pcp."faccionista" f on f.codfaccionista = fc.codfaccionista
        """
        consulta2 = pd.read_sql(sql, self.conn)
        capacidadeFaccionista = pd.merge(DataFrameBackupPlano, consulta2, on='categoria', how='left')
        capacidadeFaccionista['Capacidade/dia'].fillna(0, inplace=True)
        capacidadeFaccionista.fillna('-', inplace=True)
        capacidadeFaccionista['capacidadeSoma'] = capacidadeFaccionista.groupby('categoria')['Capacidade/dia'].transform('sum')
        capacidadeFaccionista['exedente'] = capacidadeFaccionista['Meta Dia'] - capacidadeFaccionista['capacidadeSoma']
        capacidadeFaccionista = capacidadeFaccionista[capacidadeFaccionista['exedente'] > 0].groupby('categoria').agg({'exedente': 'first'}).reset_index()
        capacidadeFaccionista.rename(columns={'exedente': '01- AcordadoDia'}, inplace=True)
        return capacidadeFaccionista

    def obter_resumo(self, capacidadeFaccionista):
        Consultafaccionistas = self.RegistroFaccionistas2()  # Presume-se que esta função esteja definida em algum lugar
        resumo = pd.concat([Consultafaccionistas, capacidadeFaccionista], ignore_index=True)
        resumo['nome'].fillna('EXCEDENTE', inplace=True)
        resumo.fillna('-', inplace=True)
        resumo['04-%Capacidade'] = resumo.groupby('categoria')['01- AcordadoDia'].transform('sum')
        resumo['04-%Capacidade'] = round(resumo['01- AcordadoDia'] / resumo['04-%Capacidade'] * 100)
        resumo = resumo.sort_values(by=['categoria', '01- AcordadoDia'], ascending=[True, False])
        consulta1 = self.carregarBackupPlanoFac()
        resumo = pd.merge(resumo, consulta1, on='categoria')

        return resumo

    def ajustar_colunas(self, resumo):
        colunas_necessarias = ['01- AcordadoDia', '04-%Capacidade', 'categoria', 'codfaccionista', 'nome', 'FaltaProgramar',
                               'Fila','dias','nomefaccionistaCsw']
        colunas_existentes = [col for col in colunas_necessarias if col in resumo.columns]
        resumo = resumo.loc[:, colunas_existentes]
        resumo['FaltaProgramar'] = resumo['FaltaProgramar'] * (resumo['04-%Capacidade'] / 100)
        resumo['Fila'] = resumo['Fila'] * (resumo['04-%Capacidade'] / 100)
        return resumo

    def obter_carga_faccionista(self):
        sql = """
            SELECT op.numeroOP, op.codProduto, d.codOP, d.codFac as codfaccionista,l.qtdPecasRem as carga, e.descricao as nome  
        FROM tco.OrdemProd op 
        left join tct.RemessaOPsDistribuicao d on d.Empresa = 1 and d.codOP = op.numeroOP and d.situac = 2 and d.codFase = op.codFaseAtual 
        left join tct.RemessasLoteOP l on l.Empresa = d.Empresa  and l.codRemessa = d.numRem 
        join tcp.Engenharia e on e.codEmpresa = 1 and e.codEngenharia = op.codProduto 
        WHERE op.codEmpresa =1 and op.situacao =3 and op.codFaseAtual in (455, 459, 429)
            """
        with ConexaoBanco.Conexao2() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()
        consulta['categoria'] = '-'
        consulta['categoria'] = consulta['nome'].apply(self.mapear_categoria)

        consulta = consulta.groupby(['categoria', 'codfaccionista']).agg({'carga': 'sum'}).reset_index()
        consulta['codfaccionista'] = consulta['codfaccionista'].astype(str)
        return consulta

    def mapear_categoria(self, nome):
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

    def calcular_metas(self, resumo, cargaFac):
        resumo = pd.merge(resumo, cargaFac, on=['categoria', 'codfaccionista'], how='left')
        resumo['carga'].fillna(0, inplace=True)
        resumo['Falta Produzir'] = resumo[['carga', 'Fila', 'FaltaProgramar']].sum(axis=1)
        resumo['Meta Dia'] = (resumo['Falta Produzir'] / resumo['dias']).round(0)

        resumo.rename(columns={
            'codfaccionista': '00- codFac',
            'nome': '01-nomeFac',
            'categoria': '03- categoria',
            '01- AcordadoDia': '04- AcordadoDia',
            '04-%Capacidade': '05-%Cap.',
            'FaltaProgramar': '06-FaltaProgramar',
            'Fila': '07-Fila',
            'carga': '08-Carga',
            'Falta Produzir': '09-Falta Produzir',
            'dias': '10-dias',
            'Meta Dia': '11-Meta Dia'}, inplace=True)

        return resumo

    def adicionar_realizacao(self, resumo):
        Realizacao = realizadoFases.RemetidoFaseCategoriaFaccionista(self.dataMovFaseIni, self.dataMovFaseFim)
        resumo = pd.merge(resumo, Realizacao, on=['03- categoria', '00- codFac'], how='left')
        resumo['Remetido'].fillna(0, inplace=True)

        Retornado = realizadoFases.RetornadoFaseCategoriaFaccionista(self.dataMovFaseIni, self.dataMovFaseFim)
        resumo = pd.merge(resumo, Retornado, on=['03- categoria', '00- codFac'], how='left')
        resumo['Realizado'].fillna(0, inplace=True)

        return resumo

    def RegistroFaccionistas2(self):
        conn = ConexaoPostgreWms.conexaoEngine()

        sql = """SELECT * FROM pcp.faccionista """
        sql2 = """SELECT * FROM pcp."faccaoCategoria" """

        sql = pd.read_sql(sql, conn)
        sql2 = pd.read_sql(sql2, conn)
        merged = pd.merge(sql, sql2, on='codfaccionista', how='left')

        merged.fillna('-', inplace=True)
        merged['nome'] = np.where(merged['apelidofaccionista'] != '-', merged['apelidofaccionista'],
                                  merged['nomefaccionista'])
        merged = merged[['Capacidade/dia', 'codfaccionista', 'nome', 'nomecategoria']]

        # Conversão e renomeação de colunas
        # Substituir valores '-' por NaN
        merged['Capacidade/dia'] = merged['Capacidade/dia'].replace('-', np.nan)

        # Remover valores não numéricos (opcional, se desejar apenas remover linhas inválidas)
        merged = merged[pd.to_numeric(merged['Capacidade/dia'], errors='coerce').notnull()]

        # Substituir valores NaN por 0 (ou outro valor padrão)
        merged['Capacidade/dia'] = merged['Capacidade/dia'].fillna(0)

        # Converter para int
        merged['Capacidade/dia'] = merged['Capacidade/dia'].astype(int)
        merged.rename(columns={'Capacidade/dia': '01- AcordadoDia', 'nomecategoria': 'categoria','nomefaccionista':'nomefaccionistaCsw'}, inplace=True)

        return merged

    def calcular_resumo(self):
        backupPLano = self.carregarBackupPlanoFac()
        consulta1_ = self.carregar_capacidades(backupPLano)
        resumo = self.obter_resumo(consulta1_)
        print(resumo.dtypes)

        resumo = self.ajustar_colunas(resumo)
        print(resumo.dtypes)

        cargaFac = self.obter_carga_faccionista()
        resumo = self.calcular_metas(resumo, cargaFac)
        resumo = self.adicionar_realizacao(resumo)
        os.system('clear')

        return resumo

