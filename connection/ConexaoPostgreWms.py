'''Arquivo para chamar a conexao de banco com o postgre'''
import psycopg2
from sqlalchemy import create_engine

def conexaoEngine(host, bancoDados):
    db_name = bancoDados
    db_user = "postgres"
    db_password = "Master100"
    db_host = host
    portbanco = "5432"

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{portbanco}/{db_name}"
    return create_engine(connection_string)