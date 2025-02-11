'''Arquivo para chamar a conexao de banco com o postgre'''
import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine

# Carregando as variáveis de ambiente do arquivo .env
load_dotenv()

def conexaoEngine():
    db_name = os.getenv('POSTGRES_DB')
    db_name = 'PCP'
    db_user = "postgres"
    db_password = "Master100"
    db_host = os.getenv('DATABASE_HOST')
    db_host ='10.162.0.190'
    portbanco = "5432"


    if not all([db_name, db_user, db_password, db_host]):
        raise ValueError("One or more environment variables are not set")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{portbanco}/{db_name}"
    return create_engine(connection_string)

def conexaoEngineWms():
    db_name = os.getenv('POSTGRES_DB')
    db_name = 'Reposicao'
    db_user = "postgres"
    db_password = "Master100"
    db_host = os.getenv('DATABASE_HOST')
    db_host ='10.162.0.190'
    portbanco = "5432"


    if not all([db_name, db_user, db_password, db_host]):
        raise ValueError("One or more environment variables are not set")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{portbanco}/{db_name}"
    return create_engine(connection_string)

def Funcao_InserirOFF (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    #database = os.getenv('POSTGRES_DB')
    database = 'PCP'
    user = "postgres"
    password = "Master100"
    #host = os.getenv('DATABASE_HOST')
    host ='10.162.0.190'
    port = "5432"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='pcp')

def conexaoInsercao():
    db_name = os.getenv('POSTGRES_DB')
    db_name = 'PCP'

    db_user = "postgres"
    db_password = "Master100"
    db_host = os.getenv('DATABASE_HOST')
    db_host ='10.162.0.190'

    portbanco = "5432"

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=portbanco)


def Funcao_InserirBackup (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    #database = os.getenv('POSTGRES_DB')
    database = 'PCP'
    user = "postgres"
    password = "Master100"
    #host = os.getenv('DATABASE_HOST')
    host ='10.162.0.190'
    port = "5432"

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='backup')