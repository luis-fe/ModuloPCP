'''Arquivo para chamar a conexao de banco com o postgre'''
import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine

# Carregando as variáveis de ambiente do arquivo .env
load_dotenv()

def conexaoEngine():
    db_name = os.getenv('POSTGRES_DB')
    db_user = "postgres"
    db_password = "Master100"
    db_host = os.getenv('DATABASE_HOST')
    portbanco = "5432"
    # Debugging prints
    print(f"DATABASE_NAME: {db_name}")
    print(f"POSTGRES_USER: {db_user}")
    print(f"POSTGRES_PASSWORD: {db_password}")
    print(f"DATABASE_HOST: {db_host}")

    if not all([db_name, db_user, db_password, db_host]):
        raise ValueError("One or more environment variables are not set")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{portbanco}/{db_name}"
    return create_engine(connection_string)