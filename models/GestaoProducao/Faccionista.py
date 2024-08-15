import gc
from models.GestaoOPAberto import realizadoFases
import pandas as pd
from connection import ConexaoPostgreWms, ConexaoBanco
import numpy as np
import os

class MetasFaccionistas:
    def __init__(self, codigoPlano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado):
        self.codigoPlano = codigoPlano
        self.arrayCodLoteCsw = arrayCodLoteCsw
        self.dataMovFaseIni = dataMovFaseIni
        self.dataMovFaseFim = dataMovFaseFim
        self.congelado = congelado
        self.conn = ConexaoPostgreWms.conexaoEngine()

    def carregar_plano(self):
        sql = """select * from "backup"."metaCategoria" where "plano" = %s and "codLote" = %s """
        codLote = self.arrayCodLoteCsw[0]
        consulta1 = pd.read_sql(sql, self.conn, params=(self.codigoPlano, codLote))
        return consulta1

    def carregar_capacidades(self):
        sql = """select nomecategoria as categoria, codfaccionista, "Capacidade/dia"::int from "PCP".pcp."faccaoCategoria" fc"""
        consulta2 = pd.read_sql(sql, self.conn)
        return consulta2

    def realizar_merge(self, consulta1, consulta2):
        consulta1_ = pd.merge(consulta1, consulta2, on='categoria', how='left')
        consulta1_['Capacidade/dia'].fillna(0, inplace=True)
        consulta1_.fillna('-', inplace=True)
        consulta1_['capacidadeSoma'] = consulta1_.groupby('categoria')['Capacidade/dia'].transform('sum')
        consulta1_['excedente'] = consulta1_['Meta Dia'] - consulta1_['capacidadeSoma']
        consulta1_ = consulta1_[consulta1_['exedente'] > 0].groupby('categoria').agg({'exedente': 'first'}).reset_index()
        consulta1_.rename(columns={'exedente': '01- AcordadoDia'}, inplace=True)
        return consulta1_

    def obter_resumo(self, consulta1_, Consultafaccionistas):
        resumo = pd.concat([Consultafaccionistas, consulta1_], ignore_index=True)
        resumo['nome'].fillna('EXCEDENTE', inplace=True)
        resumo.fillna('-', inplace=True)
        resumo['04-%Capacidade'] = resumo.groupby('categoria')['01- AcordadoDia'].transform('sum')
        resumo['04-%Capacidade'] = round(resumo['01- AcordadoDia'] / resumo['04-%Capacidade'] * 100)
        resumo = resumo.sort_values(by=['categoria', '01- AcordadoDia'], ascending=[True, False])
        return resumo

    def ajustar_colunas(self, resumo):
        colunas_necessarias = ['01- AcordadoDia', '04-%Capacidade', 'categoria', 'codfaccionista', 'nome', 'FaltaProgramar',
                               'Fila','dias']
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

    def mapear_categoria(self):

        def mapear_categoria(nome):
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
        merged.rename(columns={'Capacidade/dia': '01- AcordadoDia', 'nomecategoria': 'categoria'}, inplace=True)

        return merged

    def calcular_resumo(self):
        consulta1 = self.carregar_plano()
        consulta2 = self.carregar_capacidades()

        consulta1_ = self.realizar_merge(consulta1, consulta2)
        Consultafaccionistas = self.RegistroFaccionistas2()  # Presume-se que esta função esteja definida em algum lugar

        resumo = self.obter_resumo(consulta1_, Consultafaccionistas)
        resumo = pd.merge(resumo, consulta1, on='categoria')
        resumo = self.ajustar_colunas(resumo)

        cargaFac = self.obter_carga_faccionista()
        resumo = self.calcular_metas(resumo, cargaFac)
        resumo = self.adicionar_realizacao(resumo)

        os.system('clear')

        return resumo

# Uso da classe:
# metas = MetasFaccionistas(codigoPlano, arrayCodLoteCsw, dataMovFaseIni, dataMovFaseFim, congelado)
# resultado = metas.calcular_resumo()
